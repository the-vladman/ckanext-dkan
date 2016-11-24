import json
import urllib
import urllib2
import httplib
import datetime
import socket
import datetime

from ckanext.harvest.harvesters import CKANHarvester
from ckanext.harvest.model import HarvestObject
from ckan import model
import ckan.lib.munge as munge
from ckan.plugins import toolkit

log = __import__('logging').getLogger(__name__)

MIMETYPE_FORMATS = {
    'text/html': 'HTML',
    'text/csv': 'CSV',
    'text/xml': 'XML',
    'application/pdf': 'PDF',
    'application/zip': 'ZIP',
    'application/rdf+xml': 'RDF',
    'application/json': 'JSON',
    'application/vnd.ms-excel': 'XLS',
    'application/vnd.google-earth.kml+xml': 'KML',
    'application/msword': 'DOC',
}


class DKANHarvester(CKANHarvester):
    ckan_revision_api_works = False

    def info(self):
        return {
            'name': 'dkan',
            'title': 'DKAN HARVESTER',
            'description': 'Harvests remote DKAN instances',
            'form_config_interface': 'Text'
        }

    def _get_search_api_offset(self):
        return '%s/current_package_list_with_resources' % self._get_action_api_offset()

    def _get_all_packages(self, base_url, harvest_job):
        # Request all remote packages
        url = base_url + '/api/3/action/package_list'
        log.debug('Getting all DKAN packages: %s', url)
        try:
            content = self._get_content(url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s - %s'
                                    % (url, e), harvest_job)
            return None

        packages = json.loads(content)['result']

        return packages

    def _get_package(self, base_url, harvest_object):
        url = base_url + '/api/3/action/package_show/' + harvest_object.guid
        log.debug('Getting DKAN package: %s', url)

        # Get contents
        try:
            content = self._get_content(url)
        except Exception, e:
            self._save_object_error(
                'Unable to get content for package: %s - %r' % (url, e),
                harvest_object)
            return None, None

        package = json.loads(content)['result'][0]
        return url, json.dumps(package)

    def gather_stage(self, harvest_job):
        log.debug('In DKANHarvester gather_stage (%s)',
                  harvest_job.source.url)
        toolkit.requires_ckan_version(min_version='2.0')
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_ckan_base_url = harvest_job.source.url.rstrip('/')

        # Filter in/out datasets from particular organizations
        fq_terms = []
        org_filter_include = self.config.get('organizations_filter_include', [])
        org_filter_exclude = self.config.get('organizations_filter_exclude', [])
        if org_filter_include:
            fq_terms.append(' OR '.join(
                'organization:%s' % org_name for org_name in org_filter_include))
        elif org_filter_exclude:
            fq_terms.extend(
                '-organization:%s' % org_name for org_name in org_filter_exclude)

        # Ideally we can request from the remote CKAN only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self._last_error_free_job(harvest_job)
        log.debug('Last error-free job: %r', last_error_free_job)
        if (last_error_free_job and
                not self.config.get('force_all', False)):
            get_all_packages = False

            # Request only the datasets modified since
            last_time = last_error_free_job.gather_started
            # Note: SOLR works in UTC, and gather_started is also UTC, so
            # this should work as long as local and remote clocks are
            # relatively accurate. Going back a little earlier, just in case.
            get_changes_since = \
                (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

            fq_since_last_time = 'metadata_modified:[{since}Z TO *]' \
                .format(since=get_changes_since)

            try:
                pkg_dicts = self._search_for_datasets(
                    remote_ckan_base_url,
                    fq_terms + [fq_since_last_time])
            except SearchError, e:
                log.info('Searching for datasets changed since last time '
                         'gave an error: %s', e)
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info('No datasets have been updated on the remote '
                         'DKAN instance since the last harvest job %s',
                         last_time)
                return None

        # Fall-back option - request all the datasets from the remote CKAN
        if get_all_packages:
            # Request all remote packages
            try:
                pkg_dicts = self._search_for_datasets(remote_ckan_base_url,
                                                      fq_terms)
            except SearchError, e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote DKAN for datasets:%s url:%s'
                    'terms:%s' % (e, remote_ckan_base_url, fq_terms),
                    harvest_job)
                return None
        if not pkg_dicts:
            self._save_gather_error(
                'No datasets found at DKAN: %s' % remote_ckan_base_url,
                harvest_job)
            return None

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []
            for pkg_dict in pkg_dicts:
                if pkg_dict['id'] in package_ids:
                    log.info('Discarding duplicate dataset %s - probably due '
                             'to datasets being changed at the same time as '
                             'when the harvester was paging through',
                             pkg_dict['id'])
                    continue
                package_ids.add(pkg_dict['id'])

                log.debug('Creating HarvestObject for %s %s',
                          pkg_dict['name'], pkg_dict['id'])

                log.debug('Recurso de prueba vale verga')
                log.debug(pkg_dict['resources'])

                obj = HarvestObject(guid=pkg_dict['id'],
                                    job=harvest_job,
                                    content=json.dumps(pkg_dict))
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)

    def _search_for_datasets(self, remote_ckan_base_url, fq_terms=None):
        '''Does a dataset search on a remote CKAN and returns the results.

        Deals with paging to return all the results, not just the first page.
        '''
        base_search_url = remote_ckan_base_url + self._get_search_api_offset()
        params = {'limit': '100', 'offset': '0'}
        # There is the worry that datasets will be changed whilst we are paging
        # through them.
        # * In SOLR 4.7 there is a cursor, but not using that yet
        #   because few CKANs are running that version yet.
        # * However we sort, then new names added or removed before the current
        #   page would cause existing names on the next page to be missed or
        #   double counted.
        # * Another approach might be to sort by metadata_modified and always
        #   ask for changes since (and including) the date of the last item of
        #   the day before. However if the entire page is of the exact same
        #   time, then you end up in an infinite loop asking for the same page.
        # * We choose a balanced approach of sorting by ID, which means
        #   datasets are only missed if some are removed, which is far less
        #   likely than any being added. If some are missed then it is assumed
        #   they will harvested the next time anyway. When datasets are added,
        #   we are at risk of seeing datasets twice in the paging, so we detect
        #   and remove any duplicates.

        pkg_dicts = []
        pkg_ids = set()
        previous_content = None
        while True:
            url = base_search_url + '?' + urllib.urlencode(params)
            log.debug('Searching for CKAN datasets: %s', url)
            try:
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError(
                    'Error sending request to search remote '
                    'CKAN instance %s using URL %r. Error: %s' %
                    (remote_ckan_base_url, url, e))

            if previous_content and content == previous_content:
                raise SearchError('The paging doesn\'t seem to work. URL: %s' %
                                  url)
            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote CKAN was not JSON: %r'
                                  % content)
            try:
                pkg_dicts_page = response_dict.get('result', [])
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'result/results: %r' % response_dict)

            if len(pkg_dicts_page) == 0:
                break
            # Weed out any datasets found on previous pages (should datasets be
            # changing while we page)

            if type(pkg_dicts_page[0]) == list:
                pkg_dicts_page = pkg_dicts_page[0]

            ids_in_page = set(p['id'] for p in pkg_dicts_page)
            duplicate_ids = ids_in_page & pkg_ids
            if duplicate_ids:
                pkg_dicts_page = [self._convert_dkan_package_to_ckan(p) for p in pkg_dicts_page if p['id'] not in duplicate_ids]
            pkg_ids |= ids_in_page

            pkg_dicts.extend(pkg_dicts_page)

            params['offset'] = str(int(params['offset']) + int(params['limit']))

        return pkg_dicts

    def _convert_dkan_package_to_ckan(self, package):
        # change the DKAN-isms into CKAN-style
        try:
            if 'extras' not in package:
                package['extras'] = {}

            if 'name' not in package:
                package['name'] = munge.munge_title_to_name(package['title'])

            if 'description' in package:
                package['notes'] = package['description']

            for license in model.Package.get_license_register().values():
                if license.title == package['license_title']:
                    package['license_id'] = license.id
                    break

            if 'resources' not in package:
                raise ValueError('Dataset has no resources')

            for resource in package['resources']:
                resource['description'] = resource['title']

                if 'size' in resource:
                    if type(resource['size']) == str:
                        clean_size = resource['size'].replace('KB', '').replace('MB', '').strip()
                        resource['size'] = int(clean_size)


                try:
                    resource['created'] = self._convert_date(resource['created'])
                except:
                    log.error(
                        'Incorrect date created format in Package: {0}, Source: {1} Date: {2}'.format(package['name'], resource['title'], resource['created'])
                    )
                    resource['created'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

                try:
                    resource['last_modified'] = self._convert_date(resource['last_modified'], last_modified=True)
                except:
                    log.error(
                        'Incorrect date last_modified format in Package: {0}, Source: {1} Date: {2}'.format(package['name'], resource['title'], resource['last_modified'])
                    )
                    resource['last_modified'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

                if 'revision_id' in resource:
                    del resource['revision_id']

                if 'format' not in resource:
                    resource['format'] = MIMETYPE_FORMATS.get(resource.get('mimetype'), '')

            if 'private' in package:
                # DKAN appears to have datasets with private=True which are
                # still public: https://github.com/NuCivic/dkan/issues/950. If
                # they were really private then we'd not get be able to access
                # them, so assume they are not private.
                package['private'] = False

            log.debug("Vale verga!!")
            log.debug(package)

            return package
        except Exception, e:
            log.error('Unable to get convert DKAN to CKAN package: %s' % e)
            return None

    def _convert_date(self, date, last_modified=False):

        try:
            date_object = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
            return date
        except:
            pass

        log.debug("convirtiendo")
        try:
            date_correct_format = date.replace('Date changed\t', '')[4:] if last_modified else date[4:]
            date_object = datetime.datetime.strptime(date_correct_format, '%d/%m/%y - %I:%M')
        except Exception e:
            log.debug(e)

        return date_object.strftime("%Y-%m-%dT%H:%M:%S.%f")

    def _fix_tags(self, package_dict):
        pass
