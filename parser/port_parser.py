"""Parse IDMC ports and PowerCenter transformation field/port metadata.

Supports Ports/Port, direct Port children, and PowerCenter TRANSFORMFIELD
(or SOURCEFIELD/TARGETFIELD) elements when a mapping definition is available.
"""


class PortParser:
    PORT_TAGS = {"port", "transformfield", "sourcefield", "targetfield", "field"}
    CONTAINER_TAGS = {"ports", "transformfields", "sourcefields", "targetfields", "fields"}

    def parse(self, parent_element):
        if parent_element is None:
            return []
        results = []
        # Collect only the relevant port-like elements. Avoid parsing nested
        # child metadata twice by selecting direct children or one known container.
        containers = [c for c in list(parent_element) if self._tag(c) in self.CONTAINER_TAGS]
        if containers:
            for container in containers:
                for element in container.iter():
                    if element is not container and self._tag(element) in self.PORT_TAGS:
                        results.append(self._parse_port(element))
        else:
            for child in list(parent_element):
                if self._tag(child) in self.PORT_TAGS:
                    results.append(self._parse_port(child))
        return results

    def _parse_port(self, port):
        attrs = dict(port.attrib)
        expression = self._attr(port, "expression", "defaultvalue")
        expression_element = next((c for c in list(port) if self._tag(c) in {"expression", "expressiontext"}), None)
        if expression_element is not None:
            text = "".join(expression_element.itertext()).strip()
            if text:
                expression = text
        return {
            "name": self._attr(port, "name"),
            "datatype": self._attr(port, "datatype", "datatype", "data_type"),
            "direction": self._attr(port, "direction", "porttype", "porttype"),
            "expression": expression.strip() if isinstance(expression, str) else expression,
            "attributes": attrs,
            "extensions": [self._element_to_dict(c) for c in port
                           if self._tag(c) not in {"expression", "expressiontext"}],
        }

    @staticmethod
    def _tag(element):
        return str(element.tag).split("}")[-1].lower()

    @staticmethod
    def _attr(element, *names):
        lookup = {str(k).lower(): v for k, v in element.attrib.items()}
        for name in names:
            if name.lower() in lookup:
                return lookup[name.lower()]
        return None

    def _element_to_dict(self, element):
        data = {"tag": element.tag, "attributes": dict(element.attrib), "children": []}
        if element.text and element.text.strip():
            data["text"] = element.text.strip()
        data["children"] = [self._element_to_dict(c) for c in element]
        return data
