"""Parse target definitions from legacy/IDMC and PowerCenter repository XML.

Session writer extensions are not target definitions and are not converted
into target schemas. They remain session metadata handled by SessionParser.
"""


class TargetParser:
    TARGET_TAGS = {"target", "targetinstance"}
    COLUMN_TAGS = {"column", "field", "targetfield"}
    CONTAINER_TAGS = {"targets", "targetinstances"}

    def parse(self, folder):
        if folder is None:
            return []
        targets = []
        containers = [e for e in list(folder) if self._tag(e) in self.CONTAINER_TAGS]
        candidates = []
        for container in containers:
            candidates.extend(e for e in list(container) if self._tag(e) in self.TARGET_TAGS)
        candidates.extend(e for e in list(folder) if self._tag(e) in self.TARGET_TAGS)

        seen = set()
        for target in candidates:
            identity = (id(target), self._attr(target, "name"))
            if identity in seen:
                continue
            seen.add(identity)
            targets.append(self._parse_target(target))
        return targets

    def _parse_target(self, target):
        columns = []
        for child in list(target):
            tag = self._tag(child)
            if tag in {"columns", "fields", "targetfields"}:
                columns.extend(self._parse_columns(child))
            elif tag in self.COLUMN_TAGS:
                columns.append(self._parse_column(child))

        return {
            "id": self._attr(target, "id"),
            "name": self._attr(target, "name"),
            "type": self._attr(target, "type", "objecttype"),
            "connection": self._attr(target, "connection", "connectionname"),
            "catalog": self._attr(target, "catalog"),
            "schema": self._attr(target, "schema", "owner"),
            "target": self._attr(target, "target", "object", "name"),
            "operation": self._attr(target, "operation", "loadtype"),
            "attributes": dict(target.attrib),
            "columns": columns,
            "extensions": [self._element_to_dict(c) for c in target
                            if self._tag(c) not in self.COLUMN_TAGS | {"columns", "fields", "targetfields"}],
        }

    def _parse_columns(self, container):
        return [self._parse_column(e) for e in container.iter()
                if e is not container and self._tag(e) in self.COLUMN_TAGS]

    def _parse_column(self, column):
        return {
            "name": self._attr(column, "name"),
            "datatype": self._attr(column, "datatype", "data_type"),
            "nullable": self._attr(column, "nullable", "nullability"),
            "length": self._attr(column, "length"),
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
