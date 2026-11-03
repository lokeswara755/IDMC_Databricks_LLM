class AssetFolderParser:

    def parse(self, root):
        container = root.find(".//AssetFolders")

        if container is None:
            raise ValueError("AssetFolders not found")

        folders = [
            self._parse_folder(element)
            for element in container
            if element.tag == "Folder"
        ]

        return {
            "asset_folders": folders,
            "summary": {
                "folder_count": len(folders),
            },
        }

    def _parse_folder(self, element):
        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "attributes": dict(element.attrib),
            "extensions": [
                self._element_to_dict(child)
                for child in element
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
                AssetFolderParser._element_to_dict(child)
                for child in element
            ],
        }