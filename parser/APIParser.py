class APIParser:

    def parse(self, root):
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets not found")

        apis = [
            self._parse_api(element)
            for element in assets
            if element.tag == "API"
        ]

        return {
            "apis": apis,
            "summary": {
                "api_count": len(apis),
                "process_reference_count": sum(
                    len(api["process_refs"]) for api in apis
                ),
                "operation_count": sum(
                    len(api["operations"]) for api in apis
                ),
            },
        }

    def _parse_api(self, element):
        process_refs = []
        operations = []
        extensions = []

        for child in element:
            if child.tag == "ProcessRef":
                process_refs.append({
                    "object_id": child.get("objectId"),
                    "attributes": dict(child.attrib),
                })

            elif child.tag == "Operation":
                operations.append({
                    "method": child.get("method"),
                    "resource": child.get("resource"),
                    "attributes": dict(child.attrib),
                })

            else:
                extensions.append(self._element_to_dict(child))

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "attributes": dict(element.attrib),
            "process_refs": process_refs,
            "operations": operations,
            "extensions": extensions,
            "raw_xml": self._element_to_dict(element),
        }

    @staticmethod
    def _element_to_dict(element):
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                APIParser._element_to_dict(child)
                for child in element
            ],
        }