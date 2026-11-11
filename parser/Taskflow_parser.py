"""Parser for Informatica IDMC getResponse XML exports with application/xml+taskflow."""
from xml.etree.ElementTree import Element


def local_name(tag):
    return tag.rsplit('}', 1)[-1] if '}' in tag else tag.split(':')[-1]


def element_to_dict(element):
    return {
        'tag': local_name(element.tag),
        'namespace_tag': element.tag,
        'attributes': dict(element.attrib),
        'text': (element.text or '').strip() or None,
        'children': [element_to_dict(child) for child in list(element)],
    }


def find_first(element, name):
    if element is None:
        return None
    return next((node for node in element.iter() if local_name(node.tag) == name), None)


def text_of(element):
    if element is None:
        return None
    return ''.join(element.itertext()).strip() or None


class TaskflowParser:
    MIME_TYPE = 'application/xml+taskflow'
    NODE_NAMES = {'service', 'eventContainer', 'assignment', 'container', 'catch'}

    def can_parse(self, root):
        return text_of(find_first(root, 'MimeType')) == self.MIME_TYPE or local_name(root.tag) == 'taskflow'

    def parse(self, root):
        item = find_first(root, 'Item')
        mime_type = text_of(find_first(root, 'MimeType'))
        if item is None and local_name(root.tag) != 'taskflow':
            raise ValueError('IDMC Taskflow XML: Item element not found in getResponse envelope.')
        if mime_type and mime_type != self.MIME_TYPE:
            raise ValueError(f"Unsupported taskflow MIME type: {mime_type}")
        taskflow_el = root if local_name(root.tag) == 'taskflow' else find_first(root, 'taskflow')
        if taskflow_el is None:
            raise ValueError('IDMC Taskflow XML: taskflow element not found inside Item/Entry.')

        metadata = {}
        for field in ('EntryId', 'Name', 'MimeType', 'Description', 'VersionLabel', 'State',
                      'CreatedBy', 'CreationDate', 'ModifiedBy', 'PublicationStatus', 'GUID', 'DisplayName'):
            metadata[field.lower()] = text_of(find_first(item if item is not None else root, field))

        input_el = next((x for x in list(taskflow_el) if local_name(x.tag) == 'input'), None)
        temp_el = next((x for x in list(taskflow_el) if local_name(x.tag) == 'tempFields'), None)
        flow_el = next((x for x in list(taskflow_el) if local_name(x.tag) == 'flow'), None)
        dep_el = next((x for x in list(taskflow_el) if local_name(x.tag) == 'dependencies'), None)

        parameters = []
        if input_el is not None:
            for p in input_el.iter():
                if local_name(p.tag) == 'parameter':
                    parameters.append({'name': p.get('name'), 'type': p.get('type'),
                                       'options': [{'name': o.get('name'), 'value': text_of(o)}
                                                   for o in p.iter() if local_name(o.tag) == 'option'],
                                       'raw_xml': element_to_dict(p)})

        temp_fields = []
        if temp_el is not None:
            for f in temp_el.iter():
                if local_name(f.tag) == 'field':
                    temp_fields.append({'name': f.get('name'), 'type': f.get('type'),
                                        'attributes': dict(f.attrib), 'raw_xml': element_to_dict(f)})

        services, nodes, links, operations = [], [], [], []
        for el in taskflow_el.iter():
            tag = local_name(el.tag)
            if tag in self.NODE_NAMES:
                title = next((c for c in list(el) if local_name(c.tag) == 'title'), None)
                info = {'element_type': tag, 'id': el.get('id'), 'name': el.get('name'),
                        'type': el.get('type'), 'title': text_of(title), 'attributes': dict(el.attrib)}
                nodes.append(info)
                if tag == 'service':
                    service_name = text_of(find_first(el, 'serviceName'))
                    services.append({**info, 'service_name': service_name,
                                     'service_guid': text_of(find_first(el, 'serviceGUID')),
                                     'service_input': element_to_dict(find_first(el, 'serviceInput')) if find_first(el, 'serviceInput') is not None else None,
                                     'service_output': element_to_dict(find_first(el, 'serviceOutput')) if find_first(el, 'serviceOutput') is not None else None,
                                     'parameters': [{'name': p.get('name'), 'source': p.get('source'),
                                                     'value': text_of(p), 'raw_xml': element_to_dict(p)}
                                                    for p in el.iter() if local_name(p.tag) == 'parameter']})
            elif tag == 'link':
                links.append({'id': el.get('id'), 'target_id': el.get('targetId'),
                              'source_id': el.get('sourceId'), 'type': el.get('type'),
                              'attributes': dict(el.attrib), 'raw_xml': element_to_dict(el)})
            elif tag == 'operation':
                operations.append({'source': el.get('source'), 'target': el.get('to'),
                                   'expressions': [{'language': x.get('language'), 'text': text_of(x)}
                                                   for x in el.iter() if local_name(x.tag) == 'expression'],
                                   'raw_xml': element_to_dict(el)})

        taskflow = {
            'guid': taskflow_el.get('GUID') or taskflow_el.get('guid'),
            'name': taskflow_el.get('name'), 'display_name': taskflow_el.get('displayName'),
            'attributes': dict(taskflow_el.attrib),
            'description': text_of(find_first(taskflow_el, 'description')),
            'generator': text_of(find_first(taskflow_el, 'generator')),
            'input_parameters': parameters, 'temp_fields': temp_fields,
            'services': services, 'control_flow_nodes': nodes, 'links': links,
            'operations': operations,
            'dependencies': element_to_dict(dep_el) if dep_el is not None else None,
            'flow': element_to_dict(flow_el) if flow_el is not None else None,
            'raw_xml': element_to_dict(taskflow_el),
        }
        return {
            'format': 'IDMC_TASKFLOW', 'asset_metadata': metadata, 'taskflow': taskflow,
            'summary': {
                'asset_name': metadata.get('name') or taskflow['name'],
                'mime_type': metadata.get('mimetype') or self.MIME_TYPE,
                'taskflow_guid': taskflow['guid'],
                'parameter_count': len(parameters), 'temp_field_count': len(temp_fields),
                'service_count': len(services),
                'data_task_count': sum(s.get('service_name') == 'ICSExecuteDataTask' for s in services),
                'command_task_count': sum(s.get('service_name') == 'ICSExecuteCommandTask' for s in services),
                'control_flow_node_count': len(nodes), 'link_count': len(links),
                'operation_count': len(operations), 'mapping_definition_included': False,
            }
        }
