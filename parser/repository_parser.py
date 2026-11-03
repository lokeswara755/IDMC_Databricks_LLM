from parser.connector_parser import ConnectorParser
from parser.source_parser import SourceParser
from parser.target_parser import TargetParser
from parser.mapping_parser import MappingParser
from parser.session_parser import SessionParser


class RepositoryParser:
    """
    Supports legacy Repository/Folder XML and the newer
    IDMCMetadataRepository/Project/Assets XML.
    """

    def __init__(self):
        self.connector_parser = ConnectorParser()
        self.source_parser = SourceParser()
        self.target_parser = TargetParser()
        self.mapping_parser = MappingParser()
        self.session_parser = SessionParser()

    def parse(self, root):
        if root is None:
            raise ValueError("XML root element is empty")

        # --------------------------------------------------
        # LEGACY XML FORMAT
        # --------------------------------------------------

        repository = root.find("Repository")

        if repository is not None:
            return self._parse_legacy_repository(repository)

        # --------------------------------------------------
        # NEW XML FORMAT
        # --------------------------------------------------

        if root.tag == "IDMCMetadataRepository":
            return self._parse_metadata_repository(root)

        raise ValueError(
            f"Unsupported XML root '{root.tag}'. "
            "Expected IDMCMetadataRepository or a root "
            "containing Repository."
        )

    def _parse_legacy_repository(self, repository):
        repository_data = {
            "name": repository.get("name"),
            "attributes": dict(repository.attrib),
            "folders": []
        }

        for folder in repository.findall("Folder"):
            repository_data["folders"].append(
                self._parse_legacy_folder(folder)
            )

        return repository_data

    def _parse_legacy_folder(self, folder):
        folder_data = {
            "name": folder.get("name"),
            "attributes": dict(folder.attrib),
            "connections": [],
            "sources": [],
            "targets": [],
            "mappings": [],
            "sessions": [],
            "extensions": []
        }

        folder_data["connections"] = (
            self.connector_parser.parse(folder)
        )
        folder_data["sources"] = self.source_parser.parse(folder)
        folder_data["targets"] = self.target_parser.parse(folder)
        folder_data["mappings"] = self.mapping_parser.parse(folder)
        folder_data["sessions"] = self.session_parser.parse(folder)

        known_sections = {
            "Connections",
            "Sources",
            "Targets",
            "Mappings",
            "Sessions"
        }

        folder_data["extensions"] = [
            self._element_to_dict(child)
            for child in folder
            if child.tag not in known_sections
        ]

        return folder_data

    def _parse_metadata_repository(self, root):
        projects = root.findall("Project")

        if not projects:
            raise ValueError(
                "IDMCMetadataRepository contains no Project elements"
            )

        repository_data = {
            "name": root.get("name") or projects[0].get("name"),
            "attributes": dict(root.attrib),
            "folders": [],
            "extensions": []
        }

        for project in projects:
            repository_data["folders"].append(
                self._parse_project(project)
            )

        # Preserve any repository-level elements other than Project.
        repository_data["extensions"] = [
            self._element_to_dict(child)
            for child in root
            if child.tag != "Project"
        ]

        return repository_data

    def _parse_project(self, project):
        assets = project.find("Assets")

        folder_data = {
            # Keep "folders" compatible with MigrationPipeline.
            # In this format, each entry represents a project.
            "name": project.get("name") or project.get("id"),
            "id": project.get("id"),
            "attributes": dict(project.attrib),
            "connections": [],
            "sources": [],
            "targets": [],
            "mappings": [],
            "sessions": [],
            "extensions": []
        }

        if assets is not None:
            # Parse mapping assets using the same normalized schema
            # as the legacy parser.
            folder_data["mappings"] = self.mapping_parser.parse(
                assets
            )

            # Connections are stored as a project asset container.
            connections_container = assets.find("Connections")

            if connections_container is not None:
                folder_data["connections"] = (
                    self._parse_project_connections(
                        connections_container
                    )
                )

            # Preserve other project assets rather than discarding
            # taskflows, mapping tasks, parameters, DQ rules, APIs,
            # ingestion jobs and other metadata.
            folder_data["extensions"] = [
                self._element_to_dict(asset)
                for asset in assets
                if asset.tag not in {"Mapping", "Connections"}
            ]

        # Preserve folder definitions and any other project sections.
        known_project_sections = {"Assets"}

        folder_data["extensions"].extend(
            self._element_to_dict(child)
            for child in project
            if child.tag not in known_project_sections
        )

        return folder_data

    def _parse_project_connections(self, container):
        connections = []

        for connection in container.findall("Connection"):
            connection_data = {
                "id": connection.get("id"),
                "name": connection.get("name"),
                "type": connection.get("type"),
                "subtype": connection.get("subtype"),
                "attributes": dict(connection.attrib),
                "properties": {},
                "extensions": []
            }

            # Support Property elements when present.
            for prop in connection.findall("Property"):
                name = prop.get("name")
                if name:
                    connection_data["properties"][name] = (
                        prop.get("value")
                        if prop.get("value") is not None
                        else (prop.text or "").strip()
                    )

            connection_data["extensions"] = [
                self._element_to_dict(child)
                for child in connection
                if child.tag != "Property"
            ]

            connections.append(connection_data)

        return connections

    def _element_to_dict(self, element):
        data = {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "children": []
        }

        if element.text and element.text.strip():
            data["text"] = element.text.strip()

        for child in element:
            data["children"].append(
                self._element_to_dict(child)
            )

        return data
