import unittest
from bco_banking_unit_feasibility import partial_close_feasibility as check

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

if __name__ == '__main__': unittest.main()
