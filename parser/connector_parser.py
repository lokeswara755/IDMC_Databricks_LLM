"""Parse connection definitions, PowerCenter connection references and links.

`parse(folder)` retains the original contract: it returns connection
DEFINITIONS. PowerCenter CONNECTIONREFERENCE elements are references only;
use `parse_references(folder)` to retrieve them. Mapping-level CONNECTOR
lineage is exposed separately through `parse_dataflow_links(mapping)`.
"""


class ConnectorParser:
    CONNECTION_TAGS = {"connection"}
    CONNECTION_CONTAINERS = {"connections"}

    def parse(self, folder):
        """Parse standalone connection definitions (legacy/IDMC)."""
        if folder is None:
            return []
        containers = [e for e in list(folder) if self._tag(e) in self.CONNECTION_CONTAINERS]
        elements = []
        for container in containers:
            elements.extend(e for e in list(container) if self._tag(e) in self.CONNECTION_TAGS)
        elements.extend(e for e in list(folder) if self._tag(e) in self.CONNECTION_TAGS)

        results, seen = [], set()
        for connection in elements:
            if id(connection) in seen:
                continue
            seen.add(id(connection))
            properties = {}
            for child in connection:
                if self._tag(child) == "property":
                    name = self._attr(child, "name")
                    if name:
                        properties[name] = self._attr(child, "value") or self._text(child)
            results.append({
                "id": self._attr(connection, "id"),
                "name": self._attr(connection, "name"),
                "type": self._attr(connection, "type"),
                "subtype": self._attr(connection, "subtype"),
                "attributes": dict(connection.attrib),
                "properties": properties,
                "extensions": [self._element_to_dict(c) for c in connection
                                if self._tag(c) != "property"],
            })
        return results

    def parse_references(self, folder):
        """Extract SESSIONEXTENSION/CONNECTIONREFERENCE references read-only."""
        if folder is None:
            return []
        references = []
        for session in folder.iter():
            if self._tag(session) != "session":
                continue
            session_name = self._attr(session, "name")
            for extension in session:
                if self._tag(extension) != "sessionextension":
                    continue
                for reference in extension.iter():
                    if self._tag(reference) != "connectionreference":
                        continue
                    references.append({
                        "session_name": session_name,
                        "session_instance_name": self._attr(extension, "sinstancename"),
                        "extension_type": self._attr(extension, "type"),
                        "transformation_type": self._attr(extension, "transformationtype"),
                        "connection_name": self._attr(reference, "connectionname", "connection"),
                        "connection_type": self._attr(reference, "connectiontype", "type"),
                        "connection_subtype": self._attr(reference, "connectionsubtype", "subtype"),
                        "attributes": dict(reference.attrib),
                    })
        return references

    def parse_dataflow_links(self, mapping):
        """Parse mapping CONNECTOR edges without confusing them with connections."""
        if mapping is None:
            return []
        results = []
        for element in mapping.iter():
            if self._tag(element) != "connector":
                continue
            attrs = dict(element.attrib)
            results.append({
                "from": self._attr(element, "from", "frominstance"),
                "from_field": self._attr(element, "fromfield"),
                "to": self._attr(element, "to", "toinstance"),
                "to_field": self._attr(element, "tofield"),
                "attributes": attrs,
            })
        return results

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

    @staticmethod
    def _text(element):
        return "".join(element.itertext()).strip()

    def _element_to_dict(self, element):
        data = {"tag": element.tag, "attributes": dict(element.attrib), "children": []}
        if element.text and element.text.strip():
            data["text"] = element.text.strip()
        data["children"] = [self._element_to_dict(c) for c in element]
        return data
