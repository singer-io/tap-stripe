import stripe
import unittest
from tap_stripe.rule_map import RuleMap

FIELDS_SET = {
    'MyName123': 'my_name_123',
    'ANOTHERName': 'anothername',
    'anotherName': 'another_name',
    'add____*LPlO': 'add_lpl_o',
    '123Abc%%_opR': '123_abc_op_r',
    'UserName': 'user_name',
    'A0a_A': 'a_0_a_a',
    'aE0': 'a_e_0',
    'a.a b': 'a_a_b'
    
}


class TestRuleMap(unittest.TestCase):
    
    def test_apply_rules_to_original_field(self):
        rule_map = RuleMap()
        
        for field, value in FIELDS_SET.items():
            standard_field = rule_map.apply_rules_to_original_field(field)
            self.assertEquals(standard_field, value)