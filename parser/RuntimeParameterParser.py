
class RuntimeParameterParser:
    SUPPORTED_TAG = "FileRuntimeParameters"

    def parse(self, root):
        assets = root.find("./Project/Assets")
        if assets is None:
            raise ValueError("Project/Assets not found in XML")

        parameter_files = [
            self._parse_file(element)
            for element in list(assets)
            if element.tag == self.SUPPORTED_TAG
        ]

        parameter_count = sum(
            len(item["parameters"]) for item in parameter_files
        )

        return {
            "parameter_files": parameter_files,
            "summary": {
                "parameter_file_count": len(parameter_files),
                "parameter_count": parameter_count,
            },
        }

    def _parse_file(self, element):
        parameters = []

        for child in list(element):
            if child.tag == "Parameter":
                parameters.append({
                    "name": child.get("name"),
                    "value": child.get("value"),
                    "attributes": dict(child.attrib),
                    "raw_xml": self._element_to_dict(child),
                })

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "attributes": dict(element.attrib),
            "parameters": parameters,
            "parameter_count": len(parameters),
            "extensions": [
                self._element_to_dict(child)
                for child in list(element)
                if child.tag != "Parameter"
            ],
            "raw_xml": self._element_to_dict(element),
        }

    @staticmethod
    def _element_to_dict(element):
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                RuntimeParameterParser._element_to_dict(child)
                for child in list(element)
            ],
        }
