class MappingParser:
    """
    Normalize legacy and newer IDMC mapping XML.

    Source location attributes are exposed as first-class fields while the
    complete original attribute dictionary and unknown XML extensions remain
    available for downstream processing.
    """

    SOURCE_LOCATION_ATTRIBUTES = {
        "catalog": ("catalog", "sourceCatalog", "source_catalog"),
        "schema": ("schema", "sourceSchema", "source_schema", "owner"),
        "physical_table": (
            "physicalTable", "physical_table", "table", "tableName",
            "sourceTable", "source_table",
        ),
        "file_path": (
            "filePath", "file_path", "path", "sourcePath", "source_path",
            "fileName", "filename", "objectPath", "object_path", "uri",
        ),
        "format": ("format", "fileFormat", "file_format", "sourceFormat"),
    }

    def parse(self, parent):
        if parent is None:
            return []

        tag = self._local_name(parent.tag)
        if tag == "Mapping":
            mapping_elements = [parent]
        elif tag == "Mappings":
            mapping_elements = self._children(parent, "Mapping")
        else:
            mappings_element = self._first_child(parent, "Mappings")
            if mappings_element is not None:
                mapping_elements = self._children(mappings_element, "Mapping")
            else:
                mapping_elements = self._children(parent, "Mapping")

        return [self.parse_mapping(element) for element in mapping_elements]

    def parse_mapping(self, mapping):
        mapping_data = {
            "id": mapping.get("id"),
            "name": mapping.get("name"),
            "description": mapping.get("description"),
            "attributes": dict(mapping.attrib),
            "source_instances": [],
            "transformations": [],
            "target_instances": [],
            "links": [],
            "extensions": [],
        }

        sources_element = self._first_child(mapping, "Sources")
        if sources_element is None:
            sources_element = self._first_child(mapping, "SourceInstances")

        if sources_element is not None:
            for source in list(sources_element):
                if self._local_name(source.tag) not in {"Source", "SourceInstance"}:
                    continue

                attrs = dict(source.attrib)
                source_data = {
                    "id": source.get("id"),
                    "name": source.get("name"),
                    "source": source.get("source") or source.get("object") or source.get("name"),
                    "object": source.get("object"),
                    "type": source.get("type"),
                    "connection": source.get("connection"),
                    "catalog": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["catalog"]),
                    "schema": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["schema"]),
                    "physical_table": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["physical_table"]),
                    "file_path": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["file_path"]),
                    "format": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["format"]),
                    "attributes": attrs,
                    "columns": [],
                    "extensions": [],
                }

                fields_container = self._first_child(source, "Fields")
                if fields_container is None:
                    fields_container = self._first_child(source, "Columns")

                if fields_container is not None:
                    field_elements = list(fields_container)
                else:
                    field_elements = [
                        child for child in source
                        if self._local_name(child.tag) in {"Field", "Column"}
                    ]

                for field in field_elements:
                    if self._local_name(field.tag) not in {"Field", "Column"}:
                        continue
                    field_attrs = dict(field.attrib)
                    source_data["columns"].append({
                        "name": field.get("name"),
                        "datatype": field.get("datatype") or field.get("dataType"),
                        "nullable": field.get("nullable"),
                        "length": field.get("length"),
                        "precision": field.get("precision"),
                        "scale": field.get("scale"),
                        "attributes": field_attrs,
                        "extensions": [self._element_to_dict(child) for child in field],
                    })

                known = {"Fields", "Columns", "Field", "Column"}
                source_data["extensions"] = [
                    self._element_to_dict(child)
                    for child in source
                    if self._local_name(child.tag) not in known
                ]
                mapping_data["source_instances"].append(source_data)

        transformations_element = self._first_child(mapping, "Transformations")
        if transformations_element is not None:
            for transformation in list(transformations_element):
                tag = self._local_name(transformation.tag)
                transformation_type = (
                    transformation.get("type")
                    or (tag[:-14] if tag.endswith("Transformation") else tag)
                )
                transformation_data = {
                    "id": transformation.get("id"),
                    "name": transformation.get("name"),
                    "type": transformation_type,
                    "xml_tag": tag,
                    "attributes": dict(transformation.attrib),
                    "details": [self._element_to_dict(child) for child in transformation],
                    "ports": self._parse_ports(transformation),
                }
                mapping_data["transformations"].append(transformation_data)

        targets_element = self._first_child(mapping, "Targets")
        if targets_element is None:
            targets_element = self._first_child(mapping, "TargetInstances")

        if targets_element is not None:
            for target in list(targets_element):
                if self._local_name(target.tag) not in {"Target", "TargetInstance"}:
                    continue
                attrs = dict(target.attrib)
                target_data = {
                    "id": target.get("id"),
                    "name": target.get("name"),
                    "target": target.get("target") or target.get("object") or target.get("name"),
                    "object": target.get("object"),
                    "type": target.get("type"),
                    "connection": target.get("connection"),
                    "catalog": self._attribute_value(attrs, ("catalog", "targetCatalog", "target_catalog")),
                    "schema": self._attribute_value(attrs, ("schema", "targetSchema", "target_schema", "owner")),
                    "physical_table": self._attribute_value(attrs, self.SOURCE_LOCATION_ATTRIBUTES["physical_table"]),
                    "operation": target.get("operation"),
                    "attributes": attrs,
                    "columns": [],
                    "extensions": [],
                }

                fields_container = self._first_child(target, "Fields")
                if fields_container is None:
                    fields_container = self._first_child(target, "Columns")
                if fields_container is not None:
                    field_elements = list(fields_container)
                else:
                    field_elements = [
                        child for child in target
                        if self._local_name(child.tag) in {"Field", "Column"}
                    ]

                for field in field_elements:
                    if self._local_name(field.tag) not in {"Field", "Column"}:
                        continue
                    field_attrs = dict(field.attrib)
                    target_data["columns"].append({
                        "name": field.get("name"),
                        "datatype": field.get("datatype") or field.get("dataType"),
                        "nullable": field.get("nullable"),
                        "length": field.get("length"),
                        "precision": field.get("precision"),
                        "scale": field.get("scale"),
                        "attributes": field_attrs,
                        "extensions": [self._element_to_dict(child) for child in field],
                    })

                known = {"Fields", "Columns", "Field", "Column"}
                target_data["extensions"] = [
                    self._element_to_dict(child)
                    for child in target
                    if self._local_name(child.tag) not in known
                ]
                mapping_data["target_instances"].append(target_data)

        links_element = self._first_child(mapping, "DataFlowLinks")
        if links_element is None:
            links_element = self._first_child(mapping, "Links")
        if links_element is not None:
            for link in self._children(links_element, "Link"):
                mapping_data["links"].append({
                    "from": link.get("from"),
                    "to": link.get("to"),
                    "attributes": dict(link.attrib),
                    "extensions": [self._element_to_dict(child) for child in link],
                })

        known_sections = {
            "Sources", "SourceInstances", "Transformations", "Targets",
            "TargetInstances", "DataFlowLinks", "Links",
        }
        mapping_data["extensions"] = [
            self._element_to_dict(child)
            for child in mapping
            if self._local_name(child.tag) not in known_sections
        ]
        return mapping_data

    def _parse_ports(self, transformation):
        ports = []
        ports_container = self._first_child(transformation, "Ports")
        if ports_container is not None:
            port_elements = self._children(ports_container, "Port")
        else:
            port_elements = self._children(transformation, "Port")

        for port in port_elements:
            expression = port.get("expression")
            expression_element = self._first_child(port, "Expression")
            if expression_element is not None:
                expression_text = "".join(expression_element.itertext()).strip()
                if expression_text:
                    expression = expression_text

            ports.append({
                "name": port.get("name"),
                "datatype": port.get("datatype") or port.get("dataType"),
                "direction": port.get("direction"),
                "expression": expression,
                "attributes": dict(port.attrib),
                "extensions": [
                    self._element_to_dict(child)
                    for child in port
                    if self._local_name(child.tag) != "Expression"
                ],
            })
        return ports

    @staticmethod
    def _local_name(tag):
        return str(tag).rsplit("}", 1)[-1]

    @classmethod
    def _first_child(cls, parent, wanted):
        for child in list(parent):
            if cls._local_name(child.tag) == wanted:
                return child
        return None

    @classmethod
    def _children(cls, parent, wanted):
        return [child for child in list(parent) if cls._local_name(child.tag) == wanted]

    @staticmethod
    def _attribute_value(attributes, aliases):
        normalized = {str(key).casefold(): value for key, value in attributes.items()}
        for alias in aliases:
            value = normalized.get(alias.casefold())
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    def _element_to_dict(self, element):
        data = {
            "tag": self._local_name(element.tag),
            "attributes": dict(element.attrib),
            "children": [],
        }
        if element.text and element.text.strip():
            data["text"] = element.text.strip()
        for child in element:
            data["children"].append(self._element_to_dict(child))
        return data
