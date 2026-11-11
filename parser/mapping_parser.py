class MappingParser:
    """
    Normalize IDMC and Informatica PowerCenter mapping XML.

    Supported IDMC/legacy sections:
      Mapping / Mappings / Assets
      Sources or SourceInstances
      Transformations
      Targets or TargetInstances
      DataFlowLinks or Links

    Supported PowerCenter structures:
      FOLDER / MAPPING
        TRANSFORMATION / TRANSFORMFIELD
        INSTANCE
        CONNECTOR

    Important:
      A PowerCenter SESSION export may contain only SESSION/@MAPPINGNAME
      references and no MAPPING definitions. In that case parse() correctly
      returns an empty list. It does not fabricate mapping logic.
    """

    def parse(self, parent):
        """Return normalized mapping definitions below parent."""

        if parent is None:
            return []

        tag = self._tag(parent)

        if tag == "MAPPING":
            mapping_elements = [parent]
        elif tag == "MAPPINGS":
            mapping_elements = self._children(parent, {"MAPPING"})
        else:
            # IDMC / legacy container.
            mappings_container = self._first_child(parent, {"MAPPINGS"})
            if mappings_container is not None:
                mapping_elements = self._children(mappings_container, {"MAPPING"})
            else:
                # PowerCenter mappings are direct children of FOLDER.
                mapping_elements = self._children(parent, {"MAPPING"})

        return [self.parse_mapping(element) for element in mapping_elements]

    def parse_mapping(self, mapping):
        """Dispatch to the correct mapping format."""

        if self._tag(mapping) == "MAPPING":
            # PowerCenter exports use uppercase NAME and contain INSTANCE
            # and CONNECTOR elements. IDMC exports commonly use lowercase
            # attributes and section containers.
            if (
                self._attr(mapping, "NAME") is not None
                or self._first_child(mapping, {"INSTANCE", "CONNECTOR"}) is not None
            ):
                return self._parse_powercenter_mapping(mapping)

        return self._parse_idmc_mapping(mapping)

    # ================================================================
    # IDMC / LEGACY PARSER
    # ================================================================

    def _parse_idmc_mapping(self, mapping):
        mapping_data = {
            "id": self._attr(mapping, "id"),
            "name": self._attr(mapping, "name"),
            "description": self._attr(mapping, "description"),
            "format": "IDMC",
            "attributes": dict(mapping.attrib),
            "source_instances": [],
            "transformations": [],
            "target_instances": [],
            "links": [],
            "extensions": []
        }

        sources_element = self._first_child(mapping, {"SOURCES", "SOURCEINSTANCES"})
        if sources_element is not None:
            for source in sources_element:
                if self._tag(source) not in {"SOURCE", "SOURCEINSTANCE"}:
                    continue
                mapping_data["source_instances"].append(
                    self._parse_idmc_object(source, object_kind="source")
                )

        transformations_element = self._first_child(mapping, {"TRANSFORMATIONS"})
        if transformations_element is not None:
            for transformation in transformations_element:
                mapping_data["transformations"].append(
                    self._parse_idmc_transformation(transformation)
                )

        targets_element = self._first_child(mapping, {"TARGETS", "TARGETINSTANCES"})
        if targets_element is not None:
            for target in targets_element:
                if self._tag(target) not in {"TARGET", "TARGETINSTANCE"}:
                    continue
                mapping_data["target_instances"].append(
                    self._parse_idmc_object(target, object_kind="target")
                )

        links_element = self._first_child(mapping, {"DATAFLOWLINKS", "LINKS"})
        if links_element is not None:
            for link in links_element:
                if self._tag(link) != "LINK":
                    continue
                mapping_data["links"].append({
                    "from": self._attr(link, "from"),
                    "to": self._attr(link, "to"),
                    "attributes": dict(link.attrib),
                    "extensions": [self._element_to_dict(c) for c in link]
                })

        known = {
            "SOURCES", "SOURCEINSTANCES", "TRANSFORMATIONS",
            "TARGETS", "TARGETINSTANCES", "DATAFLOWLINKS", "LINKS"
        }
        mapping_data["extensions"] = [
            self._element_to_dict(child)
            for child in mapping
            if self._tag(child) not in known
        ]
        return mapping_data

    def _parse_idmc_object(self, element, object_kind):
        name = self._attr(element, "name")
        object_name = (
            self._attr(element, object_kind)
            or self._attr(element, "object")
            or name
        )
        data = {
            "id": self._attr(element, "id"),
            "name": name,
            object_kind: object_name,
            "object": self._attr(element, "object"),
            "type": self._attr(element, "type"),
            "connection": self._attr(element, "connection"),
            "attributes": dict(element.attrib),
            "columns": [],
            "extensions": []
        }

        fields_container = self._first_child(element, {"FIELDS", "COLUMNS"})
        field_elements = (
            list(fields_container) if fields_container is not None
            else [c for c in element if self._tag(c) in {"FIELD", "COLUMN"}]
        )

        for field in field_elements:
            if self._tag(field) not in {"FIELD", "COLUMN"}:
                continue
            data["columns"].append(self._parse_field(field))

        known = {"FIELDS", "COLUMNS", "FIELD", "COLUMN"}
        data["extensions"] = [
            self._element_to_dict(c) for c in element if self._tag(c) not in known
        ]
        return data

    def _parse_idmc_transformation(self, transformation):
        tag = self._tag(transformation)
        explicit_type = self._attr(transformation, "type")
        transformation_type = explicit_type or (
            tag[:-14] if tag.endswith("TRANSFORMATION") else tag
        )

        return {
            "id": self._attr(transformation, "id"),
            "name": self._attr(transformation, "name"),
            "type": transformation_type,
            "xml_tag": transformation.tag,
            "attributes": dict(transformation.attrib),
            "ports": self._parse_ports(transformation),
            "details": [self._element_to_dict(c) for c in transformation]
        }

    # ================================================================
    # POWERCENTER PARSER
    # ================================================================

    def _parse_powercenter_mapping(self, mapping):
        mapping_name = self._attr(mapping, "NAME") or self._attr(mapping, "name")

        data = {
            "id": self._attr(mapping, "ID") or self._attr(mapping, "id"),
            "name": mapping_name,
            "description": self._attr(mapping, "DESCRIPTION"),
            "format": "POWERCENTER",
            "attributes": dict(mapping.attrib),
            "source_instances": [],
            "transformations": [],
            "target_instances": [],
            "instances": [],
            "links": [],
            "extensions": []
        }

        # Mapping-level transformation definitions.
        for transformation in self._children(mapping, {"TRANSFORMATION"}):
            data["transformations"].append(
                self._parse_powercenter_transformation(transformation)
            )

        # Instances represent placed source/target/transformation objects.
        for instance in self._children(mapping, {"INSTANCE"}):
            instance_data = self._parse_powercenter_instance(instance)
            data["instances"].append(instance_data)

            instance_type = (
                (instance_data.get("transformation_type") or "")
                .replace("_", " ").upper()
            )
            if "SOURCE" in instance_type:
                data["source_instances"].append(instance_data)
            elif "TARGET" in instance_type:
                data["target_instances"].append(instance_data)

        # Connector-level field lineage.
        for connector in self._children(mapping, {"CONNECTOR"}):
            data["links"].append({
                "from": self._attr(connector, "FROMINSTANCE"),
                "from_field": self._attr(connector, "FROMFIELD"),
                "to": self._attr(connector, "TOINSTANCE"),
                "to_field": self._attr(connector, "TOFIELD"),
                "attributes": dict(connector.attrib),
                "extensions": [self._element_to_dict(c) for c in connector]
            })

        known = {"TRANSFORMATION", "INSTANCE", "CONNECTOR"}
        data["extensions"] = [
            self._element_to_dict(c) for c in mapping if self._tag(c) not in known
        ]
        return data

    def _parse_powercenter_transformation(self, transformation):
        fields = []
        for field in self._children(transformation, {"TRANSFORMFIELD"}):
            fields.append(self._parse_powercenter_field(field))

        return {
            "id": self._attr(transformation, "TRANSFORMATION_ID")
                  or self._attr(transformation, "ID"),
            "name": self._attr(transformation, "NAME"),
            "type": self._attr(transformation, "TYPE"),
            "xml_tag": transformation.tag,
            "attributes": dict(transformation.attrib),
            "ports": fields,
            "details": [
                self._element_to_dict(c)
                for c in transformation
                if self._tag(c) != "TRANSFORMFIELD"
            ]
        }

    def _parse_powercenter_instance(self, instance):
        return {
            "name": self._attr(instance, "NAME"),
            "transformation_name": self._attr(instance, "TRANSFORMATION_NAME"),
            "transformation_type": self._attr(instance, "TRANSFORMATION_TYPE"),
            "transformation_version": self._attr(instance, "TRANSFORMATION_VERSION"),
            "attributes": dict(instance.attrib),
            "extensions": [self._element_to_dict(c) for c in instance]
        }

    def _parse_powercenter_field(self, field):
        return {
            "name": self._attr(field, "NAME"),
            "datatype": self._attr(field, "DATATYPE"),
            "direction": self._attr(field, "PORTTYPE"),
            "expression": self._attr(field, "EXPRESSION"),
            "attributes": dict(field.attrib),
            "extensions": [self._element_to_dict(c) for c in field]
        }

    # ================================================================
    # SHARED FIELD / PORT HELPERS
    # ================================================================

    def _parse_field(self, field):
        return {
            "name": self._attr(field, "name"),
            "datatype": self._attr(field, "datatype") or self._attr(field, "dataType"),
            "nullable": self._attr(field, "nullable"),
            "length": self._attr(field, "length"),
            "precision": self._attr(field, "precision"),
            "scale": self._attr(field, "scale"),
            "attributes": dict(field.attrib),
            "extensions": [self._element_to_dict(c) for c in field]
        }

    def _parse_ports(self, transformation):
        ports = []
        ports_container = self._first_child(transformation, {"PORTS"})
        port_elements = (
            self._children(ports_container, {"PORT"})
            if ports_container is not None
            else self._children(transformation, {"PORT"})
        )

        for port in port_elements:
            expression = self._attr(port, "expression")
            expression_element = self._first_child(port, {"EXPRESSION"})
            if expression_element is not None:
                text = "".join(expression_element.itertext()).strip()
                if text:
                    expression = text

            ports.append({
                "name": self._attr(port, "name"),
                "datatype": self._attr(port, "datatype") or self._attr(port, "dataType"),
                "direction": self._attr(port, "direction"),
                "expression": expression,
                "attributes": dict(port.attrib),
                "extensions": [
                    self._element_to_dict(c)
                    for c in port
                    if self._tag(c) != "EXPRESSION"
                ]
            })
        return ports

    # ================================================================
    # XML HELPERS
    # ================================================================

    @staticmethod
    def _tag(element):
        """Return a namespace-free, uppercase XML tag."""
        tag = element.tag
        if not isinstance(tag, str):
            return ""
        return tag.rsplit("}", 1)[-1].upper()

    @staticmethod
    def _attr(element, name):
        """Read an attribute without depending on XML attribute casing."""
        wanted = name.upper()
        for key, value in element.attrib.items():
            if key.upper() == wanted:
                return value
        return None

    def _children(self, element, accepted_tags):
        if element is None:
            return []
        accepted = {tag.upper() for tag in accepted_tags}
        return [child for child in element if self._tag(child) in accepted]

    def _first_child(self, element, accepted_tags):
        children = self._children(element, accepted_tags)
        return children[0] if children else None

    def _element_to_dict(self, element):
        data = {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "children": []
        }
        if element.text and element.text.strip():
            data["text"] = element.text.strip()
        for child in element:
            data["children"].append(self._element_to_dict(child))
        return data
