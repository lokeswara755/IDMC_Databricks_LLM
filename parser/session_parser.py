import xml.etree.ElementTree as ET


class SessionParser:
    """Parse legacy IDMC session containers and PowerCenter SESSION exports."""

    def parse(self, folder):
        """Parse sessions from a legacy folder or a PowerCenter FOLDER element."""
        if folder is None:
            return []

        # PowerCenter exports store SESSION elements directly under FOLDER.
        powercenter_sessions = self._find_children(folder, "SESSION")
        if powercenter_sessions:
            return [self._parse_powercenter_session(s) for s in powercenter_sessions]

        # Legacy IDMC-style exports store Session elements under Sessions.
        sessions_element = self._find_child(folder, "Sessions")
        if sessions_element is None:
            return []

        sessions = []
        for session in self._find_children(sessions_element, "Session"):
            session_data = {
                "name": self._get_attr(session, "name"),
                "mapping": self._get_attr(session, "mapping"),
                "attributes": dict(session.attrib),
                "properties": {},
                "source_connections": [],
                "target_connections": [],
                "extensions": []
            }

            properties_element = self._find_child(session, "Properties")
            if properties_element is not None:
                for prop in self._find_children(properties_element, "Property"):
                    property_name = self._get_attr(prop, "name")
                    if property_name:
                        session_data["properties"][property_name] = self._get_attr(prop, "value")

            for source_connection in self._find_children(session, "SourceConnection"):
                session_data["source_connections"].append({
                    "source": self._get_attr(source_connection, "source"),
                    "connection": self._get_attr(source_connection, "connection"),
                    "attributes": dict(source_connection.attrib),
                    "extensions": [self._element_to_dict(c) for c in source_connection]
                })

            for target_connection in self._find_children(session, "TargetConnection"):
                session_data["target_connections"].append({
                    "target": self._get_attr(target_connection, "target"),
                    "connection": self._get_attr(target_connection, "connection"),
                    "attributes": dict(target_connection.attrib),
                    "extensions": [self._element_to_dict(c) for c in target_connection]
                })

            known_elements = {"properties", "sourceconnection", "targetconnection"}
            session_data["extensions"] = [
                self._element_to_dict(child)
                for child in session
                if self._tag(child) not in known_elements
            ]
            sessions.append(session_data)

        return sessions

    def _parse_powercenter_session(self, session):
        """Extract session runtime metadata; this is not a mapping dataflow parse."""
        data = {
            "name": self._get_attr(session, "NAME"),
            "mapping_name": self._get_attr(session, "MAPPINGNAME"),
            "valid": self._get_attr(session, "ISVALID"),
            "reusable": self._get_attr(session, "REUSABLE"),
            "version": self._get_attr(session, "VERSIONNUMBER"),
            "description": self._get_attr(session, "DESCRIPTION"),
            "attributes": dict(session.attrib),
            "transformation_instances": [],
            "connection_references": [],
            "session_extensions": [],
            "session_attributes": {},
            "config_reference": None,
            "metadata_extensions": [],
            "extensions": []
        }

        known = {
            "sesstransformationinst", "sessionextension", "attribute",
            "configreference", "metadataextension"
        }

        for instance in self._find_children(session, "SESSTRANSFORMATIONINST"):
            instance_data = {
                "instance_name": self._get_attr(instance, "SINSTANCENAME"),
                "transformation_name": self._get_attr(instance, "TRANSFORMATIONNAME"),
                "transformation_type": self._get_attr(instance, "TRANSFORMATIONTYPE"),
                "pipeline": self._get_attr(instance, "PIPELINE"),
                "stage": self._get_attr(instance, "STAGE"),
                "is_repartition_point": self._get_attr(instance, "ISREPARTITIONPOINT"),
                "partition_type": self._get_attr(instance, "PARTITIONTYPE"),
                "attributes": {},
                "flat_file": None,
                "partitions": [],
                "extensions": []
            }
            for child in instance:
                tag = self._tag(child)
                if tag == "attribute":
                    name = self._get_attr(child, "NAME")
                    if name:
                        instance_data["attributes"][name] = self._get_attr(child, "VALUE")
                elif tag == "flatfile":
                    instance_data["flat_file"] = dict(child.attrib)
                elif tag == "partition":
                    instance_data["partitions"].append(dict(child.attrib))
                else:
                    instance_data["extensions"].append(self._element_to_dict(child))
            data["transformation_instances"].append(instance_data)

        for extension in self._find_children(session, "SESSIONEXTENSION"):
            extension_data = self._element_to_dict(extension)
            data["session_extensions"].append(extension_data)

            for connection in self._find_children(extension, "CONNECTIONREFERENCE"):
                data["connection_references"].append({
                    "session_extension_name": self._get_attr(extension, "NAME"),
                    "instance_name": self._get_attr(extension, "SINSTANCENAME"),
                    "extension_type": self._get_attr(extension, "TYPE"),
                    "connection_name": self._get_attr(connection, "CONNECTIONNAME"),
                    "connection_type": self._get_attr(connection, "CONNECTIONTYPE"),
                    "connection_subtype": self._get_attr(connection, "CONNECTIONSUBTYPE"),
                    "connection_number": self._get_attr(connection, "CONNECTIONNUMBER"),
                    "variable": self._get_attr(connection, "VARIABLE"),
                    "attributes": dict(connection.attrib)
                })

        for attribute in self._find_children(session, "ATTRIBUTE"):
            name = self._get_attr(attribute, "NAME")
            if name:
                data["session_attributes"][name] = self._get_attr(attribute, "VALUE")

        config = self._find_child(session, "CONFIGREFERENCE")
        if config is not None:
            data["config_reference"] = self._element_to_dict(config)

        for metadata in self._find_children(session, "METADATAEXTENSION"):
            data["metadata_extensions"].append(self._element_to_dict(metadata))

        data["extensions"] = [
            self._element_to_dict(child)
            for child in session
            if self._tag(child) not in known
        ]
        return data

    def _element_to_dict(self, element):
        result = {
            "tag": self._tag_original(element),
            "attributes": dict(element.attrib),
            "children": []
        }
        if element.text and element.text.strip():
            result["text"] = element.text.strip()
        result["children"] = [self._element_to_dict(child) for child in element]
        return result

    @staticmethod
    def _tag(element):
        tag = element.tag
        if "}" in tag:
            tag = tag.rsplit("}", 1)[1]
        return tag.lower()

    @staticmethod
    def _tag_original(element):
        tag = element.tag
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    def _find_child(self, parent, name):
        wanted = name.lower()
        for child in list(parent):
            if self._tag(child) == wanted:
                return child
        return None

    def _find_children(self, parent, name):
        wanted = name.lower()
        return [child for child in list(parent) if self._tag(child) == wanted]

    @staticmethod
    def _get_attr(element, name):
        wanted = name.lower()
        for key, value in element.attrib.items():
            if key.lower() == wanted:
                return value
        return None
