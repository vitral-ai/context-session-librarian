"""Read Gemini's legacy JSON snapshots and append-oriented JSONL journals.

Format reference: google-gemini/gemini-cli 4ed06ed0ddc523a6d356a4ad35ddfea331041888.
No CLI execution, authentication or network access.
"""
import copy


def replay(records):
    metadata, messages, offsets = {}, {}, {}

    def put(message, offset):
        if not isinstance(message, dict) or not isinstance(message.get('id'), str):
            raise ValueError('invalid_gemini_message')
        messages[message['id']] = copy.deepcopy(message)
        offsets[message['id']] = offset

    def patch(item, offset):
        if not isinstance(item, dict) or not isinstance(item.get('id'), str):
            raise ValueError('invalid_gemini_patch')
        current = messages.get(item['id'])
        if current is None:
            return  # native loader also ignores patches to absent messages
        if 'content' in item:
            current['content'] = copy.deepcopy(item['content'])
            current['_csl_content_offset'] = offset
        for tool in item.get('toolCalls', []):
            if not isinstance(tool, dict):
                raise ValueError('invalid_gemini_tool_patch')
            for existing in current.get('toolCalls', []):
                if existing.get('id') == tool.get('id') and 'result' in tool:
                    existing['result'] = copy.deepcopy(tool['result'])
                    current.setdefault('_csl_tool_result_offsets', {})[tool['id']] = offset

    for record, offset in records:
        if '$rewindTo' in record:
            key = record['$rewindTo']
            ids = list(messages)
            remove = ids[ids.index(key):] if key in messages else ids
            for key in remove:
                messages.pop(key); offsets.pop(key, None)
        elif '$patch' in record:
            change = record['$patch']
            if not isinstance(change, dict):
                raise ValueError('invalid_gemini_patch')
            if 'id' in change:
                patch(change, offset)
            for update in change.get('updates', []):
                patch(update, offset)
            for key in change.get('removeIds', []):
                messages.pop(key, None); offsets.pop(key, None)
            if 'orderIds' in change:
                order = change['orderIds']
                if not isinstance(order, list) or any(not isinstance(v, str) for v in order):
                    raise ValueError('invalid_gemini_order')
                order = list(dict.fromkeys(key for key in order if key in messages))
                messages = {key: messages[key] for key in list(messages) if key not in order} | {
                    key: messages[key] for key in order}
        elif '$set' in record:
            update = record['$set']
            if not isinstance(update, dict):
                raise ValueError('invalid_gemini_metadata')
            if 'messages' in update:
                messages.clear(); offsets.clear()
                for message in update['messages']:
                    put(message, offset)
            metadata.update({k: v for k, v in update.items() if k != 'messages'})
        elif 'sessionId' in record and 'projectHash' in record:
            metadata.update({k: v for k, v in record.items() if k != 'messages'})
            for message in record.get('messages', []):
                put(message, offset)
        elif 'id' in record and 'type' in record:
            put(record, offset)
        else:
            raise ValueError('unsupported_gemini_record')
    return metadata, [(message, offsets[key]) for key, message in messages.items()]
