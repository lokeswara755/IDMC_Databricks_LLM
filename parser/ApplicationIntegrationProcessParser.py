
class ApplicationIntegrationProcessParser:

    def parse(self, root):
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets not found")

        processes = [
            self._parse_process(element)
            for element in assets
            if element.tag == "ApplicationIntegrationProcess"
        ]

        return {
            "processes": processes,
            "summary": {
                "process_count": len(processes),
                "input_count": sum(
                    len(p["inputs"]) for p in processes
                ),
                "output_count": sum(
                    len(p["outputs"]) for p in processes
                ),
                "step_count": sum(
                    len(p["steps"]) for p in processes
                ),
            },
        }

    def _parse_process(self, element):
        inputs = []
        outputs = []
        steps = []
        extensions = []

        for child in element:
            if child.tag == "Input":
                inputs.append(self._element_to_dict(child))
            elif child.tag == "Output":
                outputs.append(self._element_to_dict(child))
            elif child.tag == "Steps":
                # Preserve each step and all its nested XML.
                steps.extend(
                    self._element_to_dict(step)
                    for step in child
                )
            else:
                extensions.append(self._element_to_dict(child))

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "status": element.get("status"),
            "version": element.get("version"),
            "attributes": dict(element.attrib),
            "inputs": inputs,
            "outputs": outputs,
            "steps": steps,
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
                ApplicationIntegrationProcessParser._element_to_dict(child)
                for child in element
            ],
        }
