import json
import urllib
import urllib2
import httplib
import datetime
import socket

from ckanext.harvest.harvesters import CKANHarvester
from ckan import model
import ckan.lib.munge as munge

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
                #pkg_dicts_page
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'result/results: %r' % response_dict)
            print response_dict

            if len(pkg_dicts_page) == 0:
                break
            # Weed out any datasets found on previous pages (should datasets be
            # changing while we page)
            ids_in_page = set(p['id'] for p in pkg_dicts_page[0])
            duplicate_ids = ids_in_page & pkg_ids
            if duplicate_ids:
                pkg_dicts_page = [p for p in pkg_dicts_page
                                  if p['id'] not in duplicate_ids]
            pkg_ids |= ids_in_page

            pkg_dicts.extend(pkg_dicts_page)


            params['offset'] = str(int(params['offset']) + int(params['limit']))

        return pkg_dicts


    @classmethod
    def get_harvested_package_dict(cls, harvest_object):
        package = CKANHarvester.get_harvested_package_dict(harvest_object)
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

            return package
        except Exception, e:
            cls._save_object_error(
                'Unable to get convert DKAN to CKAN package: %s' % e,
                harvest_object)
            return None

    def _fix_tags(self, package_dict):
        pass
