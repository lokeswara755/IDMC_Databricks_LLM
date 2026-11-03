
from xml.etree.ElementTree import Element


class TaskflowParser:
    """
    Parses Taskflow and ReusableTaskflow assets.
    Preserves attributes, ordered steps, nested elements,
    references, conditions, and original XML structure.
    """

    SUPPORTED_TAGS = {"Taskflow", "ReusableTaskflow"}

    def parse(self, root: Element) -> dict:
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets was not found in XML")

        taskflows = []
        reusable_taskflows = []

        for element in list(assets):
            if element.tag not in self.SUPPORTED_TAGS:
                continue

            parsed = self._parse_taskflow(element)

            if element.tag == "Taskflow":
                taskflows.append(parsed)
            else:
                reusable_taskflows.append(parsed)

        return {
            "taskflows": taskflows,
            "reusable_taskflows": reusable_taskflows,
            "summary": {
                "taskflow_count": len(taskflows),
                "reusable_taskflow_count": len(reusable_taskflows)
            }
        }

    def _parse_taskflow(self, element: Element) -> dict:
        steps_container = element.find("Steps")

        steps = []

        if steps_container is not None:
            for position, step in enumerate(
                list(steps_container), start=1
            ):
                steps.append(
                    self._parse_step(step, position)
                )

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "asset_type": element.tag,
            "attributes": dict(element.attrib),
            "steps": steps,
            "step_count": len(steps),
            "extensions": [
                self._element_to_dict(child)
                for child in list(element)
                if child.tag != "Steps"
            ],
            "raw_xml": self._element_to_dict(element)
        }

    def _parse_step(self, element: Element, position: int) -> dict:
        """
        Parse any step type without assuming every step
        has the same attributes or child elements.
        """

        return {
            "position": position,
            "element_type": element.tag,
            "step_type": element.get("type"),
            "name": element.get("name"),
            "reference": (
                element.get("ref")
                or element.get("objectId")
            ),
            "condition": element.get("condition"),
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                self._element_to_dict(child)
                for child in list(element)
            ]
        }

    def _element_to_dict(self, element: Element) -> dict:
        """
        Generic recursive conversion. Preserves XML metadata
        even when a step contains an unfamiliar element.
        """

        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                self._element_to_dict(child)
                for child in list(element)
            ]
        }
