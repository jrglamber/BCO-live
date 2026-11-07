import unittest
from bco_banking_unit_feasibility import partial_close_feasibility as check, whole_trade_basket_bank

class BrokerUnitFeasibilityTests(unittest.TestCase):
    def test_one_unit_cannot_partially_bank(self):
        for fraction in (0.2, 0.33, 0.5, 0.75):
            self.assertFalse(check(1, fraction)['feasible'])
    def test_two_units_half_is_feasible(self):
        self.assertEqual(check(2, 0.5)['close_units'], 1)
        self.assertEqual(check(2, 0.5)['effective_fraction'], 0.5)
    def test_two_units_one_third_is_not_feasible(self):
        self.assertFalse(check(2, 0.33)['feasible'])
    def test_three_units_half_rounds_down(self):
        self.assertEqual(check(3, 0.5)['close_units'], 1)
    def test_nonfinite_inputs_rejected(self):
        self.assertFalse(check(float('nan'), 0.5)['feasible'])
        self.assertFalse(check(2, float('inf'))['feasible'])

    def test_basket_can_bank_whole_trade_when_partial_cannot(self):
        trades=[{'broker_trade_id':'a','units':1,'unrealised_net_gbp':10},
                {'broker_trade_id':'b','units':2,'unrealised_net_gbp':20},
                {'broker_trade_id':'c','units':1,'unrealised_net_gbp':-3}]
        result=whole_trade_basket_bank(trades,0.5)
        self.assertTrue(result['feasible'])
        self.assertEqual([t['broker_trade_id'] for t in result['selected']],['a'])
        self.assertEqual(result['banked_gbp_before_costs'],10)
    def test_basket_does_not_overbank_target(self):
        trades=[{'broker_trade_id':'a','units':1,'unrealised_net_gbp':10}]
        self.assertFalse(whole_trade_basket_bank(trades,0.2)['feasible'])
    def test_duplicate_broker_ids_fail_closed(self):
        trades=[{'broker_trade_id':'a','units':1,'unrealised_net_gbp':10}]*2
        self.assertFalse(whole_trade_basket_bank(trades,0.5)['feasible'])

if __name__ == '__main__': unittest.main()
