class MappingParser:
    """
    Parses both legacy and newer IDMC mapping XML structures.

    Legacy:
        <Mappings>
            <Mapping>
                <SourceInstances>...</SourceInstances>
                <Transformations>...</Transformations>
                <TargetInstances>...</TargetInstances>
                <Links>...</Links>
            </Mapping>
        </Mappings>

    New:
        <Assets>
            <Mapping id="..." name="...">
                <Sources>...</Sources>
                <Transformations>...</Transformations>
                <Targets>...</Targets>
                <DataFlowLinks>...</DataFlowLinks>
            </Mapping>
        </Assets>
    """

    def parse(self, parent):
        """Return normalized mappings from a folder, Assets, or Mapping."""

        if parent is None:
            return []

        if parent.tag == "Mapping":
            mapping_elements = [parent]

        elif parent.tag == "Mappings":
            mapping_elements = parent.findall("Mapping")

        else:
            # New XML: Assets contains Mapping assets.
            # Legacy XML: Folder contains a Mappings section.
            mappings_element = parent.find("Mappings")

            if mappings_element is not None:
                mapping_elements = mappings_element.findall("Mapping")
            else:
                mapping_elements = parent.findall("Mapping")

        return [
            self.parse_mapping(element)
            for element in mapping_elements
        ]

    def parse_mapping(self, mapping):
        """Normalize one Mapping element into the pipeline's data model."""

        mapping_data = {
            "id": mapping.get("id"),
            "name": mapping.get("name"),
            "description": mapping.get("description"),
            "attributes": dict(mapping.attrib),
            "source_instances": [],
            "transformations": [],
            "target_instances": [],
            "links": [],
            "extensions": []
        }

        # --------------------------------------------------
        # SOURCES
        # --------------------------------------------------

        sources_element = (
            mapping.find("Sources")
            or mapping.find("SourceInstances")
        )

        # ElementTree elements can evaluate as false when they
        # have no children. Use explicit None checks instead.
        if sources_element is None:
            sources_element = mapping.find("Sources")

        if sources_element is None:
            sources_element = mapping.find("SourceInstances")

        if sources_element is not None:
            source_tags = {"Source", "SourceInstance"}

            for source in sources_element:
                if source.tag not in source_tags:
                    continue

                source_data = {
                    "id": source.get("id"),
                    "name": source.get("name"),
                    "source": (
                        source.get("source")
                        or source.get("object")
                        or source.get("name")
                    ),
                    "object": source.get("object"),
                    "type": source.get("type"),
                    "connection": source.get("connection"),
                    "attributes": dict(source.attrib),
                    "columns": [],
                    "extensions": []
                }

                # Support both <Fields><Field/></Fields>
                # and <Columns><Column/></Columns>.
                fields_container = source.find("Fields")
                if fields_container is None:
                    fields_container = source.find("Columns")

                if fields_container is not None:
                    field_elements = list(fields_container)
                else:
                    field_elements = [
                        child for child in source
                        if child.tag in {"Field", "Column"}
                    ]

                for field in field_elements:
                    if field.tag not in {"Field", "Column"}:
                        continue

                    source_data["columns"].append({
                        "name": field.get("name"),
                        "datatype": (
                            field.get("datatype")
                            or field.get("dataType")
                        ),
                        "nullable": field.get("nullable"),
                        "length": field.get("length"),
                        "precision": field.get("precision"),
                        "scale": field.get("scale"),
                        "attributes": dict(field.attrib),
                        "extensions": [
                            self._element_to_dict(child)
                            for child in field
                        ]
                    })

                known_source_children = {
                    "Fields", "Columns", "Field", "Column"
                }

                source_data["extensions"] = [
                    self._element_to_dict(child)
                    for child in source
                    if child.tag not in known_source_children
                ]

                mapping_data["source_instances"].append(source_data)

        # --------------------------------------------------
        # TRANSFORMATIONS
        # --------------------------------------------------

        transformations_element = mapping.find("Transformations")

        if transformations_element is not None:
            for transformation in transformations_element:
                tag = transformation.tag

                # New XML uses tags such as
                # ExpressionTransformation and FilterTransformation.
                # Legacy XML may use <Transformation type="Expression"/>.
                transformation_type = (
                    transformation.get("type")
                    or (
                        tag[:-14]
                        if tag.endswith("Transformation")
                        else tag
                    )
                )

                transformation_data = {
                    "id": transformation.get("id"),
                    "name": transformation.get("name"),
                    "type": transformation_type,
                    "xml_tag": tag,
                    "attributes": dict(transformation.attrib),

                    # Preserve transformation-specific logic, including
                    # conditions, derived fields, lookup rules, groupings,
                    # router groups, rank/window details and DQ rules.
                    "details": [
                        self._element_to_dict(child)
                        for child in transformation
                    ]
                }

                # Preserve legacy port metadata if present.
                transformation_data["ports"] = self._parse_ports(
                    transformation
                )

                mapping_data["transformations"].append(
                    transformation_data
                )

        # --------------------------------------------------
        # TARGETS
        # --------------------------------------------------

        targets_element = mapping.find("Targets")
        if targets_element is None:
            targets_element = mapping.find("TargetInstances")

        if targets_element is not None:
            for target in targets_element:
                if target.tag not in {"Target", "TargetInstance"}:
                    continue

                target_data = {
                    "id": target.get("id"),
                    "name": target.get("name"),
                    "target": (
                        target.get("target")
                        or target.get("object")
                        or target.get("name")
                    ),
                    "object": target.get("object"),
                    "type": target.get("type"),
                    "connection": target.get("connection"),
                    "catalog": target.get("catalog"),
                    "schema": target.get("schema"),
                    "operation": target.get("operation"),
                    "attributes": dict(target.attrib),
                    "columns": [],
                    "extensions": []
                }

                fields_container = target.find("Fields")
                if fields_container is None:
                    fields_container = target.find("Columns")

                if fields_container is not None:
                    field_elements = list(fields_container)
                else:
                    field_elements = [
                        child for child in target
                        if child.tag in {"Field", "Column"}
                    ]

                for field in field_elements:
                    if field.tag not in {"Field", "Column"}:
                        continue

                    target_data["columns"].append({
                        "name": field.get("name"),
                        "datatype": (
                            field.get("datatype")
                            or field.get("dataType")
                        ),
                        "nullable": field.get("nullable"),
                        "length": field.get("length"),
                        "precision": field.get("precision"),
                        "scale": field.get("scale"),
                        "attributes": dict(field.attrib),
                        "extensions": [
                            self._element_to_dict(child)
                            for child in field
                        ]
                    })

                known_target_children = {
                    "Fields", "Columns", "Field", "Column"
                }

                target_data["extensions"] = [
                    self._element_to_dict(child)
                    for child in target
                    if child.tag not in known_target_children
                ]

                mapping_data["target_instances"].append(target_data)

        # --------------------------------------------------
        # LINKS
        # --------------------------------------------------

        links_element = mapping.find("DataFlowLinks")
        if links_element is None:
            links_element = mapping.find("Links")

        if links_element is not None:
            for link in links_element.findall("Link"):
                mapping_data["links"].append({
                    "from": link.get("from"),
                    "to": link.get("to"),
                    "attributes": dict(link.attrib),
                    "extensions": [
                        self._element_to_dict(child)
                        for child in link
                    ]
                })

        # --------------------------------------------------
        # OTHER MAPPING-LEVEL METADATA
        # --------------------------------------------------

        known_sections = {
            "Sources",
            "SourceInstances",
            "Transformations",
            "Targets",
            "TargetInstances",
            "DataFlowLinks",
            "Links"
        }

        mapping_data["extensions"] = [
            self._element_to_dict(child)
            for child in mapping
            if child.tag not in known_sections
        ]

        return mapping_data

    def _parse_ports(self, transformation):
        """Keep legacy direct and container-based port definitions."""

        ports = []
        ports_container = transformation.find("Ports")

        if ports_container is not None:
            port_elements = ports_container.findall("Port")
        else:
            port_elements = transformation.findall("Port")

        for port in port_elements:
            expression = port.get("expression")
            expression_element = port.find("Expression")

            if expression_element is not None:
                text = "".join(expression_element.itertext()).strip()
                if text:
                    expression = text

            ports.append({
                "name": port.get("name"),
                "datatype": (
                    port.get("datatype")
                    or port.get("dataType")
                ),
                "direction": port.get("direction"),
                "expression": expression,
                "attributes": dict(port.attrib),
                "extensions": [
                    self._element_to_dict(child)
                    for child in port
                    if child.tag != "Expression"
                ]
            })

        return ports

    def _element_to_dict(self, element):
        """Recursively preserve XML attributes, text and child elements."""

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
