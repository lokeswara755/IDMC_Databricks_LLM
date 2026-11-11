
from parser.connector_parser import ConnectorParser
from parser.source_parser import SourceParser
from parser.target_parser import TargetParser
from parser.mapping_parser import MappingParser
from parser.session_parser import SessionParser


class RepositoryParser:
    """
    Supports three XML structures:

    1. Legacy:
       Repository -> Folder

    2. IDMC metadata:
       IDMCMetadataRepository -> Project -> Assets

    3. Informatica PowerCenter:
       POWERMART -> REPOSITORY -> FOLDER
           -> SESSION
           -> WORKFLOW
           -> TASKINSTANCE

    The PowerCenter export may contain session references to mappings
    without containing the actual mapping definitions. These references
    are preserved separately from mapping definitions.
    """

    def __init__(self):
        self.connector_parser = ConnectorParser()
        self.source_parser = SourceParser()
        self.target_parser = TargetParser()
        self.mapping_parser = MappingParser()
        self.session_parser = SessionParser()

    # ==========================================================
    # PUBLIC ENTRY POINT
    # ==========================================================

    def parse(self, root):
        if root is None:
            raise ValueError("XML root element is empty")

        root_tag = self._tag(root)

        # ------------------------------------------------------
        # FORMAT 1: POWERCENTER XML
        # Check POWERMART BEFORE searching for a child named
        # Repository. Otherwise the nested REPOSITORY element is
        # incorrectly routed through the legacy parser.
        # ------------------------------------------------------

        if root_tag == "powermart":
            return self._parse_powercenter_repository(root)

        # ------------------------------------------------------
        # FORMAT 2: IDMC METADATA XML
        # ------------------------------------------------------

        if root_tag == "idmcmetadatarepository":
            return self._parse_metadata_repository(root)

        # ------------------------------------------------------
        # FORMAT 3: LEGACY XML
        # ------------------------------------------------------

        repository = self._find_child(root, "Repository")

        if repository is not None:
            return self._parse_legacy_repository(repository)

        if root_tag == "repository":
            return self._parse_legacy_repository(root)

        raise ValueError(
            f"Unsupported XML root '{root.tag}'. "
            "Supported roots: Repository, "
            "IDMCMetadataRepository, POWERMART."
        )

    # ==========================================================
    # LEGACY REPOSITORY
    # ==========================================================

    def _parse_legacy_repository(self, repository):
        repository_data = {
            "name": self._get_attr(repository, "name"),
            "attributes": dict(repository.attrib),
            "folders": []
        }

        for folder in self._find_children(repository, "Folder"):
            repository_data["folders"].append(
                self._parse_legacy_folder(folder)
            )

        return repository_data

    def _parse_legacy_folder(self, folder):
        folder_data = {
            "name": self._get_attr(folder, "name"),
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

        folder_data["sources"] = (
            self.source_parser.parse(folder)
        )

        folder_data["targets"] = (
            self.target_parser.parse(folder)
        )

        folder_data["mappings"] = (
            self.mapping_parser.parse(folder)
        )

        folder_data["sessions"] = (
            self.session_parser.parse(folder)
        )

        known_sections = {
            "connections",
            "sources",
            "targets",
            "mappings",
            "sessions"
        }

        folder_data["extensions"] = [
            self._element_to_dict(child)
            for child in folder
            if self._tag(child) not in known_sections
        ]

        return folder_data

    # ==========================================================
    # IDMC METADATA REPOSITORY
    # ==========================================================

    def _parse_metadata_repository(self, root):
        projects = self._find_children(root, "Project")

        if not projects:
            raise ValueError(
                "IDMCMetadataRepository contains no Project elements"
            )

        repository_data = {
            "name": self._get_attr(root, "name")
                    or self._get_attr(projects[0], "name"),
            "attributes": dict(root.attrib),
            "folders": [],
            "extensions": []
        }

        for project in projects:
            repository_data["folders"].append(
                self._parse_project(project)
            )

        repository_data["extensions"] = [
            self._element_to_dict(child)
            for child in root
            if self._tag(child) != "project"
        ]

        return repository_data

    def _parse_project(self, project):
        assets = self._find_child(project, "Assets")

        folder_data = {
            "name": (
                self._get_attr(project, "name")
                or self._get_attr(project, "id")
            ),
            "id": self._get_attr(project, "id"),
            "attributes": dict(project.attrib),
            "connections": [],
            "sources": [],
            "targets": [],
            "mappings": [],
            "sessions": [],
            "extensions": []
        }

        if assets is not None:
            folder_data["mappings"] = (
                self.mapping_parser.parse(assets)
            )

            connections_container = self._find_child(
                assets, "Connections"
            )

            if connections_container is not None:
                folder_data["connections"] = (
                    self._parse_project_connections(
                        connections_container
                    )
                )

            folder_data["extensions"] = [
                self._element_to_dict(asset)
                for asset in assets
                if self._tag(asset) not in {
                    "mapping",
                    "connections"
                }
            ]

        folder_data["extensions"].extend(
            self._element_to_dict(child)
            for child in project
            if self._tag(child) != "assets"
        )

        return folder_data

    def _parse_project_connections(self, container):
        connections = []

        for connection in self._find_children(
            container, "Connection"
        ):
            connection_data = {
                "id": self._get_attr(connection, "id"),
                "name": self._get_attr(connection, "name"),
                "type": self._get_attr(connection, "type"),
                "subtype": self._get_attr(connection, "subtype"),
                "attributes": dict(connection.attrib),
                "properties": {},
                "extensions": []
            }

            for prop in self._find_children(connection, "Property"):
                name = self._get_attr(prop, "name")

                if name:
                    value = self._get_attr(prop, "value")

                    if value is None:
                        value = (prop.text or "").strip()

                    connection_data["properties"][name] = value

            connection_data["extensions"] = [
                self._element_to_dict(child)
                for child in connection
                if self._tag(child) != "property"
            ]

            connections.append(connection_data)

        return connections

    # ==========================================================
    # POWERCENTER REPOSITORY
    # ==========================================================

    def _parse_powercenter_repository(self, root):
        repository = self._find_child(root, "REPOSITORY")

        if repository is None:
            raise ValueError(
                "POWERMART contains no REPOSITORY element"
            )

        repository_data = {
            "name": self._get_attr(repository, "NAME"),
            "attributes": dict(repository.attrib),
            "folders": [],
            "mapping_references": [],
            "format": "POWERCENTER"
        }

        folders = self._find_children(repository, "FOLDER")

        if not folders:
            raise ValueError(
                "PowerCenter REPOSITORY contains no FOLDER elements"
            )

        for folder in folders:
            folder_data = self._parse_powercenter_folder(folder)
            repository_data["folders"].append(folder_data)
            repository_data["mapping_references"].extend(
                folder_data["mapping_references"]
            )

        all_sessions = [
            session
            for folder in repository_data["folders"]
            for session in folder["sessions"]
        ]
        all_workflows = [
            workflow
            for folder in repository_data["folders"]
            for workflow in folder["workflows"]
        ]

        repository_data["summary"] = {
            "folder_count": len(repository_data["folders"]),
            "session_count": len(all_sessions),
            "folder_session_count": len(all_sessions),
            "workflow_session_count": 0,
            "total_session_count": len(all_sessions),
            "workflow_count": len(all_workflows),
            "workflow_task_instance_count": sum(
                len(workflow["tasks"]) for workflow in all_workflows
            ),
            "workflow_link_count": sum(
                len(workflow.get("workflow_links", []))
                for workflow in all_workflows
            ),
            "workflow_variable_count": sum(
                len(workflow["workflow_variables"])
                for workflow in all_workflows
            ),
            "session_transformation_instance_count": sum(
                len(session["transformation_instances"])
                for session in all_sessions
            ),
            "session_extension_count": sum(
                len(session["session_extensions"])
                for session in all_sessions
            ),
            "connection_reference_count": sum(
                len(session["connection_references"])
                for session in all_sessions
            ),
            "mapping_reference_count": len(
                repository_data["mapping_references"]
            ),
            "unique_mapping_reference_count": len({
                item["mapping_name"]
                for item in repository_data["mapping_references"]
            }),
            "mapping_definition_count": 0,
            "mapping_definitions_present": False
        }

        return repository_data

    def _parse_powercenter_folder(self, folder):
        """
        Extract session-level PowerCenter metadata.

        Important:
        SESSTRANSFORMATIONINST represents a transformation instance
        used by a session. It is not the complete mapping definition.
        """

        folder_data = {
            "name": self._get_attr(folder, "NAME"),
            "attributes": dict(folder.attrib),

            "connections": [],
            "sources": [],
            "targets": [],
            "mappings": [],
            "mapping_references": [],
            "sessions": [],

            "workflows": [],
            "tasks": [],
            "configurations": [],

            "extensions": []
        }

        mapping_references = []
        connection_references = {}

        # ------------------------------------------------------
        # SESSION EXTRACTION
        # ------------------------------------------------------

        for session in self._find_children(folder, "SESSION"):
            session_data = self._parse_powercenter_session(session)

            folder_data["sessions"].append(session_data)

            mapping_name = session_data.get("mapping_name")

            if mapping_name:
                mapping_references.append({
                    "mapping_name": mapping_name,
                    "session_name": session_data["name"],
                    "scope": "FOLDER_SESSION"
                })

            # Aggregate source and target instances.
            for instance in session_data["transformation_instances"]:
                instance_type = (
                    instance.get("transformation_type") or ""
                ).lower()

                instance_record = {
                    **instance,
                    "session_name": session_data["name"],
                    "mapping_name": mapping_name
                }

                if instance_type == "source definition":
                    folder_data["sources"].append(instance_record)

                elif instance_type == "target definition":
                    folder_data["targets"].append(instance_record)

            # Aggregate connection references.
            for connection in session_data["connection_references"]:
                key = (
                    connection.get("variable"),
                    connection.get("connection_type"),
                    connection.get("connection_subtype")
                )

                connection_references[key] = {
                    **connection,
                    "session_name": session_data["name"],
                    "mapping_name": mapping_name
                }

        folder_data["connections"] = list(
            connection_references.values()
        )

        # These are references only. Do not create fake mapping
        # definitions when the export does not contain them.
        folder_data["mapping_references"] = mapping_references

        # ------------------------------------------------------
        # WORKFLOW EXTRACTION
        # ------------------------------------------------------

        for workflow in self._find_children(folder, "WORKFLOW"):
            workflow_data = self._parse_powercenter_workflow(
                workflow
            )

            folder_data["workflows"].append(workflow_data)

        # ------------------------------------------------------
        # TASK EXTRACTION
        # ------------------------------------------------------

        for task in self._find_children(folder, "TASKINSTANCE"):
            folder_data["tasks"].append(
                self._parse_powercenter_task(task)
            )

        # ------------------------------------------------------
        # CONFIGURATION EXTRACTION
        # ------------------------------------------------------

        for config in self._find_children(folder, "CONFIG"):
            folder_data["configurations"].append(
                self._element_to_dict(config)
            )

        # ------------------------------------------------------
        # PRESERVE UNPROCESSED METADATA
        # ------------------------------------------------------

        known_sections = {
            "session",
            "workflow",
            "taskinstance",
            "config"
        }

        folder_data["extensions"] = [
            self._element_to_dict(child)
            for child in folder
            if self._tag(child) not in known_sections
        ]

        return folder_data

    # ==========================================================
    # POWERCENTER SESSION
    # ==========================================================

    def _parse_powercenter_session(self, session):
        session_data = {
            "name": self._get_attr(session, "NAME"),
            "mapping_name": self._get_attr(
                session, "MAPPINGNAME"
            ),
            "valid": self._get_attr(session, "ISVALID"),
            "reusable": self._get_attr(session, "REUSABLE"),
            "version": self._get_attr(session, "VERSIONNUMBER"),
            "attributes": dict(session.attrib),

            "transformation_instances": [],
            "connection_references": [],
            "session_extensions": [],
            "session_attributes": {},
            "config_reference": None,
            "extensions": []
        }

        # ------------------------------------------------------
        # TRANSFORMATION INSTANCES
        # ------------------------------------------------------

        for instance in self._find_children(
            session, "SESSTRANSFORMATIONINST"
        ):
            instance_data = {
                "instance_name": self._get_attr(
                    instance, "SINSTANCENAME"
                ),
                "transformation_name": self._get_attr(
                    instance, "TRANSFORMATIONNAME"
                ),
                "transformation_type": self._get_attr(
                    instance, "TRANSFORMATIONTYPE"
                ),
                "pipeline": self._get_attr(instance, "PIPELINE"),
                "stage": self._get_attr(instance, "STAGE"),
                "is_repartition_point": self._get_attr(
                    instance, "ISREPARTITIONPOINT"
                ),
                "partition_type": self._get_attr(
                    instance, "PARTITIONTYPE"
                ),
                "attributes": {},
                "flat_file": None,
                "extensions": []
            }

            for attribute in self._find_children(
                instance, "ATTRIBUTE"
            ):
                name = self._get_attr(attribute, "NAME")

                if name:
                    instance_data["attributes"][name] = (
                        self._get_attr(attribute, "VALUE")
                    )

            flat_file = self._find_child(instance, "FLATFILE")

            if flat_file is not None:
                instance_data["flat_file"] = dict(
                    flat_file.attrib
                )

            instance_data["extensions"] = [
                self._element_to_dict(child)
                for child in instance
                if self._tag(child) not in {
                    "attribute",
                    "flatfile",
                    "partition"
                }
            ]

            session_data["transformation_instances"].append(
                instance_data
            )

        # ------------------------------------------------------
        # SESSION EXTENSIONS AND CONNECTION REFERENCES
        # ------------------------------------------------------

        for extension in self._find_children(
            session, "SESSIONEXTENSION"
        ):
            extension_data = {
                "name": self._get_attr(extension, "NAME"),
                "instance_name": self._get_attr(
                    extension, "SINSTANCENAME"
                ),
                "type": self._get_attr(extension, "TYPE"),
                "subtype": self._get_attr(extension, "SUBTYPE"),
                "transformation_type": self._get_attr(
                    extension, "TRANSFORMATIONTYPE"
                ),
                "attributes": {},
                "connection_references": [],
                "extensions": []
            }

            for attribute in self._find_children(
                extension, "ATTRIBUTE"
            ):
                name = self._get_attr(attribute, "NAME")

                if name:
                    extension_data["attributes"][name] = (
                        self._get_attr(attribute, "VALUE")
                    )

            for reference in self._find_children(
                extension, "CONNECTIONREFERENCE"
            ):
                reference_data = {
                    "name": self._get_attr(
                        reference, "CNXREFNAME"
                    ),
                    "connection_name": self._get_attr(
                        reference, "CONNECTIONNAME"
                    ),
                    "connection_type": self._get_attr(
                        reference, "CONNECTIONTYPE"
                    ),
                    "connection_subtype": self._get_attr(
                        reference, "CONNECTIONSUBTYPE"
                    ),
                    "variable": self._get_attr(
                        reference, "VARIABLE"
                    ),
                    "attributes": dict(reference.attrib)
                }

                extension_data["connection_references"].append(
                    reference_data
                )

                session_data["connection_references"].append(
                    reference_data
                )

            extension_data["extensions"] = [
                self._element_to_dict(child)
                for child in extension
                if self._tag(child) not in {
                    "attribute",
                    "connectionreference"
                }
            ]

            session_data["session_extensions"].append(
                extension_data
            )

        # ------------------------------------------------------
        # SESSION-LEVEL ATTRIBUTES
        # ------------------------------------------------------

        for attribute in self._find_children(session, "ATTRIBUTE"):
            name = self._get_attr(attribute, "NAME")

            if name:
                session_data["session_attributes"][name] = (
                    self._get_attr(attribute, "VALUE")
                )

        # ------------------------------------------------------
        # CONFIG REFERENCE
        # ------------------------------------------------------

        config_reference = self._find_child(
            session, "CONFIGREFERENCE"
        )

        if config_reference is not None:
            session_data["config_reference"] = dict(
                config_reference.attrib
            )

        # ------------------------------------------------------
        # PRESERVE OTHER SESSION ELEMENTS
        # ------------------------------------------------------

        known_sections = {
            "sesstransformationinst",
            "sessionextension",
            "attribute",
            "configreference",
            "metadataextension"
        }

        session_data["extensions"] = [
            self._element_to_dict(child)
            for child in session
            if self._tag(child) not in known_sections
        ]

        return session_data

    # ==========================================================
    # POWERCENTER WORKFLOW
    # ==========================================================

    def _parse_powercenter_workflow(self, workflow):
        workflow_data = {
            "name": self._get_attr(workflow, "NAME"),
            "description": self._get_attr(
                workflow, "DESCRIPTION"
            ),
            "attributes": dict(workflow.attrib),
            "tasks": [],
            "workflow_variables": [],
            "workflow_attributes": {},
            "extensions": []
        }

        for task in self._find_children(
            workflow, "TASKINSTANCE"
        ):
            workflow_data["tasks"].append(
                self._parse_powercenter_task(task)
            )

        for variable in self._find_children(
            workflow, "WORKFLOWVARIABLE"
        ):
            workflow_data["workflow_variables"].append(
                dict(variable.attrib)
            )

        for attribute in self._find_children(
            workflow, "ATTRIBUTE"
        ):
            name = self._get_attr(attribute, "NAME")

            if name:
                workflow_data["workflow_attributes"][name] = (
                    self._get_attr(attribute, "VALUE")
                )

        known_sections = {
            "taskinstance",
            "workflowvariable",
            "attribute",
            "metadataextension"
        }

        workflow_data["extensions"] = [
            self._element_to_dict(child)
            for child in workflow
            if self._tag(child) not in known_sections
        ]

        return workflow_data

    # ==========================================================
    # POWERCENTER TASK
    # ==========================================================

    def _parse_powercenter_task(self, task):
        task_data = {
            "name": self._get_attr(task, "NAME"),
            "task_name": self._get_attr(task, "TASKNAME"),
            "task_type": self._get_attr(task, "TASKTYPE"),
            "enabled": self._get_attr(task, "ISENABLED"),
            "reusable": self._get_attr(task, "REUSABLE"),
            "attributes": dict(task.attrib),
            "values": [],
            "extensions": []
        }

        for value_pair in self._find_children(task, "VALUEPAIR"):
            task_data["values"].append(
                dict(value_pair.attrib)
            )

        for attribute in self._find_children(task, "ATTRIBUTE"):
            task_data.setdefault("task_attributes", {})[
                self._get_attr(attribute, "NAME")
            ] = self._get_attr(attribute, "VALUE")

        known_sections = {
            "valuepair",
            "attribute",
            "sessionextension"
        }

        task_data["extensions"] = [
            self._element_to_dict(child)
            for child in task
            if self._tag(child) not in known_sections
        ]

        return task_data

    # ==========================================================
    # COMMON XML HELPERS
    # ==========================================================

    def _tag(self, element):
        """
        Return a lowercase local XML tag name.

        Handles namespaced tags as well as ordinary XML tags.
        """
        if element is None:
            return ""

        tag = element.tag

        if "}" in tag:
            tag = tag.rsplit("}", 1)[-1]

        if ":" in tag:
            tag = tag.rsplit(":", 1)[-1]

        return tag.lower()

    def _find_child(self, parent, tag_name):
        if parent is None:
            return None

        expected = tag_name.lower()

        for child in parent:
            if self._tag(child) == expected:
                return child

        return None

    def _find_children(self, parent, tag_name):
        if parent is None:
            return []

        expected = tag_name.lower()

        return [
            child
            for child in parent
            if self._tag(child) == expected
        ]

    def _get_attr(self, element, attr_name):
        """
        Retrieve an XML attribute without depending on
        the capitalization used in the source XML.
        """
        if element is None:
            return None

        expected = attr_name.lower()

        for key, value in element.attrib.items():
            if key.lower() == expected:
                return value

        return None

    def _element_to_dict(self, element):
        data = {
            "tag": self._tag(element),
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
