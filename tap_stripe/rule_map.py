import re
import singer
import logging

# These are standard keys defined in the JSON Schema spec
STANDARD_KEYS = [
    'selected',
    'inclusion',
    'description',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
    'maxLength',
    'minLength',
    'format',
    'type',
    'additionalProperties',
    'anyOf',
    'patternProperties',
]


LOGGER = singer.get_logger()

class RuleMap:
    GetStdFieldsFromApiFields = {}


    def fill_rule_map_object_by_catalog(self, stream_name, stream_metadata):
        self.GetStdFieldsFromApiFields[stream_name] = {}

        for key, value in stream_metadata.items():
            api_name = value.get('original-name')
            if api_name and key:
                self.GetStdFieldsFromApiFields[stream_name][key[:-1] + (api_name,)] = key[-1:][0]

    def apply_ruleset_on_schema(self, schema, stream_name, parent = ()):
        """
        Apply defined rule set on schema and return it.
        """
        temp_dict = {}
        roll_back_dict = {}

        if schema and isinstance(schema, dict) and schema.get('properties'):
            for key in schema['properties'].keys():
                breadcrumb = parent + ('properties', key)

                self.apply_ruleset_on_schema(schema['properties'][key], stream_name, breadcrumb)

                # Skip keys available in STANDARD_KEYS
                if key not in STANDARD_KEYS:
                    standard_key = self.apply_rules_to_original_field(key)

                    if key != standard_key:
                        if standard_key not in temp_dict:
                            self.GetStdFieldsFromApiFields[stream_name][parent + ('properties', standard_key)] = key
                            temp_dict[key] = standard_key
                        else:
                            LOGGER.warning(f' Conflict found for field : {breadcrumb}')
                            roll_back_dict[standard_key] = True

        elif schema.get('anyOf'):
            for sc in schema.get('anyOf'):
                self.apply_ruleset_on_schema(sc, stream_name, parent)
        elif schema and isinstance(schema, dict) and schema.get('items'):
            breadcrumb = parent + ('items',)
            self.apply_ruleset_on_schema(schema['items'], stream_name, breadcrumb)
                
        for key, new_key in temp_dict.items():
            if roll_back_dict.get(new_key):
                breadcrumb = parent + ('properties', new_key)
                # flag for selection
                del self.GetStdFieldsFromApiFields[stream_name][breadcrumb]
                LOGGER.warning(f' Conflict found for field : {parent + ("properties", key)}')
            else:
                old_val = schema['properties'][key]
                del schema['properties'][key] # Remove old key
                schema['properties'][new_key] = old_val # Add standard_field with same value  
        
        return schema

    def apply_rule_set_on_stream_name(self, stream_name):
        standard_stream_name = self.apply_rules_to_original_field(stream_name)
        
        if stream_name != standard_stream_name:
            LOGGER.info(f'{stream_name}')
            self.GetStdFieldsFromApiFields[stream_name]['stream_name'] = stream_name
            return standard_stream_name
    
        return stream_name

    def apply_rules_to_original_field(self, key):

        standard_key = re.findall('[A-Z]*[^A-Z]*', key)
        standard_key = '_'.join(standard_key)
        
        standard_key = re.findall(r'[A-Za-z]+|\d+', standard_key)
        standard_key = '_'.join(standard_key)
        
        return standard_key.lower()

    def apply_ruleset_on_api_response(self, response, stream_name, parent = ()):
        """
        Apply defined rule set on api response and return it.
        """
        temp_dict = {}
        if type(response) == dict:
            for key, value in response.items():
                if type(value) == list and value:
                    if parent == ():
                        parent = parent  + ('properties', key)
                    breadcrumb = parent + ('items',)
                    for vl in value:
                        self.apply_ruleset_on_api_response(vl, stream_name, breadcrumb)
                elif type(value) == dict:
                    breadcrumb = parent  + ('properties', key)
                    self.apply_ruleset_on_api_response(value, stream_name, breadcrumb)
                else:
                    breadcrumb = parent + ('properties', key)
                
                if breadcrumb in self.GetStdFieldsFromApiFields[stream_name]:
                    # field found in rule_map
                    temp_dict[key] = self.GetStdFieldsFromApiFields[stream_name][breadcrumb]

            # Updated key in record
            for key, new_key in temp_dict.items():
                val = response[key]
                del response[key] # Remove old key
                response[new_key] = val # Add standard_field with same value
        
        return response
