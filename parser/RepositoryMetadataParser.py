"""Repository parser router for IDMC Taskflow, IDMC metadata and PowerCenter XML."""
from parser.powercenter_repository_parser import PowerCenterRepositoryParser
from parser.Taskflow_parser import TaskflowParser, local_name


class RepositoryParser:
    def __init__(self):
        self._legacy_components = None
        self.powercenter_parser = PowerCenterRepositoryParser()
        self.taskflow_parser = TaskflowParser()

    def _load_legacy_components(self):
        if self._legacy_components is not None:
            return self._legacy_components
        from parser.connector_parser import ConnectorParser
        from parser.source_parser import SourceParser
        from parser.target_parser import TargetParser
        from parser.mapping_parser import MappingParser
        from parser.session_parser import SessionParser
        self._legacy_components = {'connector': ConnectorParser(), 'source': SourceParser(),
                                   'target': TargetParser(), 'mapping': MappingParser(),
                                   'session': SessionParser()}
        return self._legacy_components

    def parse(self, root):
        if root is None:
            raise ValueError('XML root is None.')
        root_name = local_name(root.tag).upper()
        if root_name == 'POWERMART':
            return self.powercenter_parser.parse(root)
        if self.taskflow_parser.can_parse(root):
            return self.taskflow_parser.parse(root)
        if root_name == 'IDMCMETADATAREPOSITORY':
            return self._parse_metadata_repository(root)
        repository = next((n for n in root.iter() if local_name(n.tag).lower() == 'repository'), None)
        if repository is not None:
            return self._parse_legacy_repository(repository)
        raise ValueError(f"Unsupported XML root '{root.tag}'. Detected format is not PowerCenter, "
                         "IDMC Taskflow (application/xml+taskflow), IDMCMetadataRepository, "
                         "or a legacy Repository export.")

    def _parse_legacy_repository(self, repository):
        p = self._load_legacy_components()
        folders = [self._parse_legacy_folder(f, p) for f in list(repository)
                   if local_name(f.tag).upper() == 'FOLDER']
        return {'format': 'LEGACY_REPOSITORY', 'name': repository.get('name') or repository.get('NAME'),
                'attributes': dict(repository.attrib), 'folders': folders}

    def _parse_legacy_folder(self, folder, p):
        def first(*names):
            return next((n for n in folder.iter() if local_name(n.tag) in names), None)
        connections, sources = first('Connections', 'CONNECTIONS'), first('Sources', 'SOURCES')
        targets, mappings = first('Targets', 'TARGETS'), first('Mappings', 'MAPPINGS')
        sessions = first('Sessions', 'SESSIONS')
        return {'name': folder.get('name') or folder.get('NAME') or 'Unknown',
                'attributes': dict(folder.attrib),
                'connections': p['connector'].parse(connections) if connections is not None else [],
                'sources': p['source'].parse(sources) if sources is not None else [],
                'targets': p['target'].parse(targets) if targets is not None else [],
                'mappings': p['mapping'].parse(mappings if mappings is not None else folder),
                'sessions': p['session'].parse(sessions if sessions is not None else folder)}

    def _parse_metadata_repository(self, root):
        p = self._load_legacy_components()
        folders = [self._parse_legacy_folder(f, p) for f in root.iter()
                   if local_name(f.tag) == 'Folder']
        return {'format': 'IDMC_METADATA_REPOSITORY',
                'name': root.get('name') or root.get('id') or 'IDMCMetadataRepository',
                'attributes': dict(root.attrib), 'folders': folders}
