"""Parse source definitions from legacy/IDMC and PowerCenter repository XML.

PowerCenter SESSION/SESSIONEXTENSION reader entries are runtime/session
references, not SOURCE definitions. They are intentionally not promoted to
source definitions by this parser.
"""


class SourceParser:
    SOURCE_TAGS = {"source", "sourceinstance"}
    COLUMN_TAGS = {"column", "field", "sourcefield"}
    CONTAINER_TAGS = {"sources", "sourceinstances"}

    def parse(self, folder):
        if folder is None:
            return []

        sources = []
        # Legacy/IDMC containers, plus direct definitions where present.
        containers = [e for e in list(folder) if self._tag(e) in self.CONTAINER_TAGS]
        candidates = []
        for container in containers:
            candidates.extend(e for e in list(container) if self._tag(e) in self.SOURCE_TAGS)
        candidates.extend(e for e in list(folder) if self._tag(e) in self.SOURCE_TAGS)

        seen = set()
        for source in candidates:
            identity = (id(source), source.get("NAME") or source.get("name"))
            if identity in seen:
                continue
            seen.add(identity)
            sources.append(self._parse_source(source))
        return sources

    def _parse_source(self, source):
        columns = []
        column_tags = self.COLUMN_TAGS
        for child in list(source):
            if self._tag(child) in {"columns", "fields", "sourcefields"}:
                columns.extend(self._parse_columns(child, column_tags))
            elif self._tag(child) in column_tags:
                columns.append(self._parse_column(child))

        name = self._attr(source, "name")
        return {
            "id": self._attr(source, "id"),
            "name": name,
            "type": self._attr(source, "type", "objecttype"),
            "connection": self._attr(source, "connection", "connectionname"),
            "source": self._attr(source, "source", "object", "name"),
            "attributes": dict(source.attrib),
            "columns": columns,
            "extensions": [self._element_to_dict(c) for c in source
                            if self._tag(c) not in column_tags | {"columns", "fields", "sourcefields"}],
        }

    def _parse_columns(self, container, allowed_tags):
        result = []
        for element in container.iter():
            if element is not container and self._tag(element) in allowed_tags:
                result.append(self._parse_column(element))
        return result

    def _parse_column(self, column):
        return {
            "name": self._attr(column, "name"),
            "datatype": self._attr(column, "datatype", "datatype", "data_type"),
            "nullable": self._attr(column, "nullable", "nullability"),
            "length": self._attr(column, "length", "precision"),
            "precision": self._attr(column, "precision"),
            "scale": self._attr(column, "scale"),
            "attributes": dict(column.attrib),
            "extensions": [self._element_to_dict(c) for c in column],
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
