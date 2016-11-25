import json
import urllib
import urllib2
import httplib
import datetime
import socket
import datetime

from ckanext.harvest.harvesters import CKANHarvester
from ckanext.harvest.model import HarvestObject
from ckan.logic import ValidationError, NotFound, get_action
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
            log.debug('Searching for DKAN datasets: %s', url)
            try:
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError(
                    'Error sending request to search remote '
                    'DKAN instance %s using URL %r. Error: %s' %
                    (remote_ckan_base_url, url, e))

            if previous_content and content == previous_content:
                raise SearchError('The paging doesn\'t seem to work. URL: %s' %
                                  url)
            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote DKAN was not JSON: %r'
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

            pkg_dicts_page = [self._convert_dkan_package_to_ckan(p) for p in pkg_dicts_page]

            ids_in_page = set(p['id'] for p in pkg_dicts_page)
            duplicate_ids = ids_in_page & pkg_ids
            if duplicate_ids:
                pkg_dicts_page = [p for p in pkg_dicts_page if p['id'] not in duplicate_ids]
            pkg_ids |= ids_in_page

            pkg_dicts.extend(pkg_dicts_page)

            params['offset'] = str(int(params['offset']) + int(params['limit']))

        return pkg_dicts

    def import_stage(self, harvest_object):
        log.debug('In CKANHarvester import_stage')

        base_context = {'model': model, 'session': model.Session,
                        'user': self._get_user_name()}
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                for group_ in package_dict['groups']:
                    try:
                        data_dict = {'id': group_['id']}
                        group = get_action('group_show')(base_context.copy(), data_dict)
                        validated_groups.append({'id': group['id'], 'name': group['name']})

                    except NotFound, e:
                        log.info('Group %s is not available', group_)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_)
                            except RemoteResourceError:
                                log.error('Could not get remote group %s', group_)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)

                            get_action('group_create')(base_context.copy(), group)
                            log.info('Group %s has been newly created', group_)
                            validated_groups.append({'id': group['id'], 'name': group['name']})

                package_dict['groups'] = validated_groups

            # Local harvest source organization
            source_dataset = get_action('package_show')(base_context.copy(), {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if not remote_orgs in ('only_local', 'create'):
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                if not 'owner_org' in package_dict:
                    package_dict['owner_org'] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict['owner_org']

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(base_context.copy(), data_dict)
                        validated_org = org['id']
                    except NotFound, e:
                        log.info('Organization %s is not available', remote_org)
                        if remote_orgs == 'create':
                            try:
                                try:
                                    org = self._get_organization(harvest_object.source.url, remote_org)
                                except RemoteResourceError:
                                    # fallback if remote CKAN exposes organizations as groups
                                    # this especially targets older versions of CKAN
                                    org = self._get_group(harvest_object.source.url, remote_org)

                                for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                    org.pop(key, None)
                                get_action('organization_create')(base_context.copy(), org)
                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []
                existing_group_ids = [g['id'] for g in package_dict['groups']]
                package_dict['groups'].extend(
                    [g for g in self.config['default_group_dicts']
                     if g['id'] not in existing_group_ids])

            # Set default extras if needed
            default_extras = self.config.get('default_extras', {})
            def get_extra(key, package_dict):
                for extra in package_dict.get('extras', []):
                    if extra['key'] == key:
                        return extra
            if default_extras:
                override_extras = self.config.get('override_extras', False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = []
                for key, value in default_extras.iteritems():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict['extras'].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=
                            harvest_object.job.source.url.strip('/'),
                            harvest_source_title=
                            harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict['id'])

                    package_dict['extras'].append({'key': key, 'value': value})

            for resource in package_dict.get('resources', []):
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                resource.pop('url_type', None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop('revision_id', None)

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form='package_show')

            return result
        except ValidationError, e:
            self._save_object_error('Invalid package with GUID %s: %r' %
                                    (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%s' % e, harvest_object, 'Import')

    def _convert_dkan_package_to_ckan(self, package):
        # change the DKAN-isms into CKAN-style
        resources = []
        try:
            if 'extras' not in package:
                package['extras'] = {}

            if 'name' not in package:
                if 'title' in package:
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
                resource['description'] = resource['name']

                if 'size' in resource:
                    if type(resource['size']) == unicode or type(resource['size']) == str:
                        clean_size = resource['size'].replace('KB', '').replace('MB', '').strip()
                        try:
                            resource['size'] = float(clean_size)
                        except:
                            log.error(u'Incorrect size file format Package: {0}, Resource: {1}'.format(package['name'], resource['name']))
                            resource['size'] = 0.0

                try:
                    resource['created'] = self._convert_date(resource['created'])
                except:
                    log.error(
                        u'Incorrect date created format in Package: {0}, Source: {1} Date: {2}'.format(package['name'], resource['name'], resource['created'])
                    )
                    resource['created'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

                try:
                    resource['last_modified'] = self._convert_date(resource['last_modified'], last_modified=True)
                except:
                    log.error(
                        u'Incorrect date last_modified format in Package: {0}, Source: {1} Date: {2}'.format(package['name'], resource['name'], resource['last_modified'])
                    )
                    resource['last_modified'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

                if 'revision_id' in resource:
                    del resource['revision_id']

                if 'format' not in resource:
                    resource['format'] = MIMETYPE_FORMATS.get(resource.get('mimetype'), '')

                resources.append(resource)

            package['resources'] = resources

            if 'private' in package:
                # DKAN appears to have datasets with private=True which are
                # still public: https://github.com/NuCivic/dkan/issues/950. If
                # they were really private then we'd not get be able to access
                # them, so assume they are not private.
                package['private'] = False

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

        date_correct_format = date.replace('Date changed\t', '')[4:].lstrip() if last_modified else date[4:].lstrip()
        date_object = datetime.datetime.strptime(date_correct_format, '%m/%d/%Y - %H:%M')

        return date_object.strftime("%Y-%m-%dT%H:%M:%S.%f")

    def _fix_tags(self, package_dict):
        pass

class SearchError(Exception):
    pass
