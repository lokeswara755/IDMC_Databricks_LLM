
"""Routes XML exports to the appropriate parser."""

from parser.powercenter_repository_parser import PowerCenterRepositoryParser
from parser.Taskflow_parser import TaskflowParser, local_name


class RepositoryParser:

    def __init__(self):
        self._legacy_components = None
        self.powercenter_parser = PowerCenterRepositoryParser()
        self.taskflow_parser = TaskflowParser()

    def parse(self, root):

        if root is None:
            raise ValueError("XML root is None.")

        root_name = local_name(root.tag).upper()

        # PowerCenter XML
        if root_name == "POWERMART":
            return self.powercenter_parser.parse(root)

        # IDMC Taskflow XML
        if self.taskflow_parser.can_parse(root):
            return self.taskflow_parser.parse(root)

        # IDMC metadata repository
        if root_name == "IDMCMETADATAREPOSITORY":
            return self._parse_metadata_repository(root)

        # Legacy repository XML
        repository = next(
            (
                node for node in root.iter()
                if local_name(node.tag).lower() == "repository"
            ),
            None
        )

        if repository is not None:
            return self._parse_legacy_repository(repository)

        raise ValueError(
            f"Unsupported XML root '{root.tag}'. "
            "Expected PowerCenter, IDMC Taskflow, "
            "IDMCMetadataRepository, or legacy Repository XML."
        )

    def _load_legacy_components(self):

        if self._legacy_components is not None:
            return self._legacy_components

        from parser.connector_parser import ConnectorParser
        from parser.source_parser import SourceParser
        from parser.target_parser import TargetParser
        from parser.mapping_parser import MappingParser
        from parser.session_parser import SessionParser

        self._legacy_components = {
            "connector": ConnectorParser(),
            "source": SourceParser(),
            "target": TargetParser(),
            "mapping": MappingParser(),
            "session": SessionParser(),
        }

        return self._legacy_components

    def _parse_legacy_repository(self, repository):

        parsers = self._load_legacy_components()
        folders = []

        for folder in list(repository):

            if local_name(folder.tag).upper() != "FOLDER":
                continue

            folders.append(
                self._parse_legacy_folder(folder, parsers)
            )

        return {
            "format": "LEGACY_REPOSITORY",
            "name": repository.get("name"),
            "attributes": dict(repository.attrib),
            "folders": folders,
        }

    def _parse_legacy_folder(self, folder, parsers):

        def find_section(section_name):
            for node in folder.iter():
                if local_name(node.tag).lower() == section_name.lower():
                    return node
            return None

        connections = find_section("Connections")
        sources = find_section("Sources")
        targets = find_section("Targets")
        mappings = find_section("Mappings")
        sessions = find_section("Sessions")

        return {
            "name": folder.get("name") or "Unknown",
            "attributes": dict(folder.attrib),

            "connections": (
                parsers["connector"].parse(connections)
                if connections is not None else []
            ),

            "sources": (
                parsers["source"].parse(sources)
                if sources is not None else []
            ),

            "targets": (
                parsers["target"].parse(targets)
                if targets is not None else []
            ),

            "mappings": (
                parsers["mapping"].parse(mappings)
                if mappings is not None else []
            ),

            "sessions": (
                parsers["session"].parse(sessions)
                if sessions is not None else []
            ),
        }

    def _parse_metadata_repository(self, root):

        parsers = self._load_legacy_components()
        folders = []

        for folder in root.iter():

            if local_name(folder.tag) == "Folder":
                folders.append(
                    self._parse_legacy_folder(folder, parsers)
                )

        return {
            "format": "IDMC_METADATA_REPOSITORY",
            "name": root.get("name") or "IDMCMetadataRepository",
            "attributes": dict(root.attrib),
            "folders": folders,
        }
