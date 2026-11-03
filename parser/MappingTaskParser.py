
from xml.etree.ElementTree import Element


class MappingTaskParser:
    """Parse MappingTask assets and preserve their configuration."""

    def parse(self, root: Element) -> dict:
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets was not found in XML")

        tasks = [
            self._parse_task(element)
            for element in list(assets)
            if element.tag == "MappingTask"
        ]

        return {
            "mapping_tasks": tasks,
            "summary": {
                "mapping_task_count": len(tasks),
                "mapping_references": sum(
                    len(task["mapping_refs"]) for task in tasks
                ),
                "parameter_file_references": sum(
                    len(task["parameter_file_refs"]) for task in tasks
                )
            }
        }

    def _parse_task(self, element: Element) -> dict:
        mapping_refs = []
        parameter_file_refs = []

        for child in list(element):
            if child.tag == "MappingRef":
                mapping_refs.append(self._parse_reference(child))
            elif child.tag == "ParameterFileRef":
                parameter_file_refs.append(
                    self._parse_reference(child)
                )

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "status": element.get("status"),
            "attributes": dict(element.attrib),
            "mapping_refs": mapping_refs,
            "parameter_file_refs": parameter_file_refs,
            "extensions": [
                self._element_to_dict(child)
                for child in list(element)
                if child.tag not in {"MappingRef", "ParameterFileRef"}
            ],
            "raw_xml": self._element_to_dict(element)
        }

    @staticmethod
    def _parse_reference(element: Element) -> dict:
        return {
            "element_type": element.tag,
            "object_id": element.get("objectId"),
            "attributes": dict(element.attrib),
            "raw_xml": MappingTaskParser._element_to_dict(element)
        }

    @staticmethod
    def _element_to_dict(element: Element) -> dict:
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                MappingTaskParser._element_to_dict(child)
                for child in list(element)
            ]
        }
