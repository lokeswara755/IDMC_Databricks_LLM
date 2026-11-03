class AIRecommendationParser:

    def parse(self, root):
        container = next(
            (
                element for element in root.iter()
                if element.tag == "AIAssistedRecommendations"
            ),
            None,
        )

        if container is None:
            raise ValueError("AIAssistedRecommendations not found")

        recommendations = [
            self._element_to_dict(element)
            for element in container
        ]

        return {
            "recommendations": recommendations,
            "summary": {
                "recommendation_count": len(recommendations),
            },
        }

    @staticmethod
    def _element_to_dict(element):
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                AIRecommendationParser._element_to_dict(child)
                for child in element
            ],
        }