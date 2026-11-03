class RepositoryMetadataParser:

    def parse(self, root):
        element = next(
            (
                child for child in root
                if child.tag == "RepositoryMetadata"
            ),
            None,
        )

        if element is None:
            raise ValueError("RepositoryMetadata not found")

        note_element = element.find("Note")

        return {
            "attributes": dict(element.attrib),
            "note": (
                (note_element.text or "").strip()
                if note_element is not None
                else None
            ),
            "extensions": [
                self._element_to_dict(child)
                for child in element
                if child.tag != "Note"
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
                RepositoryMetadataParser._element_to_dict(child)
                for child in element
            ],
        }