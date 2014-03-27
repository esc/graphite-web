import copy
import datetime


from django.test import TestCase
from mock import patch, call, MagicMock
import numpy as np
import numpy.testing as npt


from graphite.render.datalib import TimeSeries
from graphite.render import functions

def return_greater(series, value):
    return [i for i in series if i is not None and i > value]

def return_less(series, value):
    return [i for i in series if i is not None and i < value]


class FunctionsTest(TestCase):
    def test_highest_max(self):
        config = [20, 50, 30, 40]
        seriesList = [range(max_val) for max_val in config]

        # Expect the test results to be returned in decending order
        expected = [
            [seriesList[1]],
            [seriesList[1], seriesList[3]],
            [seriesList[1], seriesList[3], seriesList[2]],
            # Test where num_return == len(seriesList)
            [seriesList[1], seriesList[3], seriesList[2], seriesList[0]],
            # Test where num_return > len(seriesList)
            [seriesList[1], seriesList[3], seriesList[2], seriesList[0]],
        ]
        for index, test in enumerate(expected):
            results = functions.highestMax({}, seriesList, index + 1)
            self.assertEqual(test, results)

    def test_highest_max_empty_series_list(self):
        # Test the function works properly with an empty seriesList provided.
        self.assertEqual([], functions.highestMax({}, [], 1))

    def testGetPercentile(self):
        seriesList = [
            ([None, None, 15, 20, 35, 40, 50], 20),
            (range(100), 30),
            (range(200), 60),
            (range(300), 90),
            (range(1, 101), 31),
            (range(1, 201), 61),
            (range(1, 301), 91),
            (range(0, 102), 30),
            (range(1, 203), 61),
            (range(1, 303), 91),
        ]
        for index, conf in enumerate(seriesList):
            series, expected = conf
            result = functions._getPercentile(series, 30)
            self.assertEqual(expected, result, 'For series index <%s> the 30th percentile ordinal is not %d, but %d ' % (index, expected, result))

    def test_n_percentile(self):
        seriesList = []
        config = [
            [15, 35, 20, 40, 50],
            range(1, 101),
            range(1, 201),
            range(1, 301),
            range(0, 100),
            range(0, 200),
            range(0, 300),
            # Ensure None values in list has no effect.
            [None, None, None] + range(0, 300),
        ]

        for i, c in enumerate(config):
            seriesList.append(TimeSeries('Test(%d)' % i, 0, 1, 1, c))

        def n_percentile(perc, expected):
            result = functions.nPercentile({}, seriesList, perc)
            self.assertEqual(expected, result)

        n_percentile(30, [[20], [31], [61], [91], [30], [60], [90], [90]])
        n_percentile(90, [[50], [91], [181], [271], [90], [180], [270], [270]])
        n_percentile(95, [[50], [96], [191], [286], [95], [190], [285], [285]])

    def test_sorting_by_total(self):
        seriesList = []
        config = [[1000, 100, 10, 0], [1000, 100, 10, 1]]
        for i, c in enumerate(config):
            seriesList.append(TimeSeries('Test(%d)' % i, 0, 0, 0, c))

        self.assertEqual(1110, functions.safeSum(seriesList[0]))

        result = functions.sortByTotal({}, seriesList)

        self.assertEqual(1111, functions.safeSum(result[0]))
        self.assertEqual(1110, functions.safeSum(result[1]))

    def _generate_series_list(self):
        seriesList = []
        config = [range(101), range(101), [1, None, None, None, None]]

        for i, c in enumerate(config):
            name = "collectd.test-db{0}.load.value".format(i + 1)
            seriesList.append(TimeSeries(name, 0, 1, 1, c))
        return seriesList

    def test_remove_above_percentile(self):
        seriesList = self._generate_series_list()
        percent = 50
        results = functions.removeAbovePercentile({}, seriesList, percent)
        for result in results:
            self.assertListEqual(return_greater(result, percent), [])

    def test_remove_below_percentile(self):
        seriesList = self._generate_series_list()
        percent = 50
        results = functions.removeBelowPercentile({}, seriesList, percent)
        expected = [[], [], [1]]

        for i, result in enumerate(results):
            self.assertListEqual(return_less(result, percent), expected[i])

    def test_remove_above_value(self):
        seriesList = self._generate_series_list()
        value = 5
        results = functions.removeAboveValue({}, seriesList, value)
        for result in results:
            self.assertListEqual(return_greater(result, value), [])

    def test_remove_below_value(self):
        seriesList = self._generate_series_list()
        value = 5
        results = functions.removeBelowValue({}, seriesList, value)
        for result in results:
            self.assertListEqual(return_less(result, value), [])

    def test_limit(self):
        seriesList = self._generate_series_list()
        limit = len(seriesList) - 1
        results = functions.limit({}, seriesList, limit)
        self.assertEqual(len(results), limit,
            "More than {0} results returned".format(limit),
        )

    def _verify_series_options(self, seriesList, name, value):
        """
        Verify a given option is set and True for each series in a
        series list
        """
        for series in seriesList:
            self.assertIn(name, series.options)
            if value is True:
                test_func = self.assertTrue
            else:
                test_func = self.assertEqual

            test_func(series.options.get(name), value)

    def test_second_y_axis(self):
        seriesList = self._generate_series_list()
        results = functions.secondYAxis({}, seriesList)
        self._verify_series_options(results, "secondYAxis", True)

    def test_draw_as_infinite(self):
        seriesList = self._generate_series_list()
        results = functions.drawAsInfinite({}, seriesList)
        self._verify_series_options(results, "drawAsInfinite", True)

    def test_line_width(self):
        seriesList = self._generate_series_list()
        width = 10
        results = functions.lineWidth({}, seriesList, width)
        self._verify_series_options(results, "lineWidth", width)

    def test_transform_null(self):
        seriesList = self._generate_series_list()
        transform = -5
        results = functions.transformNull({}, copy.deepcopy(seriesList), transform)

        for counter, series in enumerate(seriesList):
            if not None in series:
                continue
            # If the None values weren't transformed, there is a problem
            self.assertNotIn(None, results[counter],
                "tranformNull should remove all None values",
            )
            # Anywhere a None was in the original series, verify it
            # was transformed to the given value it should be.
            for i, value in enumerate(series):
                if value is None:
                    result_val = results[counter][i]
                    self.assertEqual(transform, result_val,
                        "Transformed value should be {0}, not {1}".format(transform, result_val),
                    )

    def test_alias(self):
        seriesList = self._generate_series_list()
        substitution = "Ni!"
        results = functions.alias({}, seriesList, substitution)
        for series in results:
            self.assertEqual(series.name, substitution)

    def test_alias_sub(self):
        seriesList = self._generate_series_list()
        substitution = "Shrubbery"
        results = functions.aliasSub({}, seriesList, "^\w+", substitution)
        for series in results:
            self.assertTrue(series.name.startswith(substitution),
                    "aliasSub should replace the name with {0}".format(substitution),
            )

    # TODO: Add tests for * globbing and {} matching to this
    def test_alias_by_node(self):
        seriesList = self._generate_series_list()

        def verify_node_name(*nodes):
            if isinstance(nodes, int):
                node_number = [nodes]

            # Use deepcopy so the original seriesList is unmodified
            results = functions.aliasByNode({}, copy.deepcopy(seriesList), *nodes)

            for i, series in enumerate(results):
                fragments = seriesList[i].name.split('.')
                # Super simplistic. Doesn't match {thing1,thing2}
                # or glob with *, both of what graphite allow you to use
                expected_name = '.'.join([fragments[i] for i in nodes])
                self.assertEqual(series.name, expected_name)

        verify_node_name(1)
        verify_node_name(1, 0)
        verify_node_name(-1, 0)

        # Verify broken input causes broken output
        with self.assertRaises(IndexError):
            verify_node_name(10000)

    def test_alpha(self):
        seriesList = self._generate_series_list()
        alpha = 0.5
        results = functions.alpha({}, seriesList, alpha)
        self._verify_series_options(results, "alpha", alpha)

    def test_color(self):
        seriesList = self._generate_series_list()
        color = "red"
        # Leave the original seriesList unmodified
        results = functions.color({}, copy.deepcopy(seriesList), color)

        for i, series in enumerate(results):
            self.assertTrue(hasattr(series, "color"),
                "The transformed seriesList is missing the 'color' attribute",
            )
            self.assertFalse(hasattr(seriesList[i], "color"),
                "The original seriesList shouldn't have a 'color' attribute",
            )
            self.assertEqual(series.color, color)

    def test_scale(self):
        seriesList = self._generate_series_list()
        multiplier = 2
        # Leave the original seriesList undisturbed for verification
        results = functions.scale({}, copy.deepcopy(seriesList), multiplier)
        for i, series in enumerate(results):
            for counter, value in enumerate(series):
                if value is None:
                    continue
                original_value = seriesList[i][counter]
                expected_value = original_value * multiplier
                self.assertEqual(value, expected_value)

    def _generate_mr_series(self):
        seriesList = [
            TimeSeries('group.server1.metric1',0,1,1,[None]),
            TimeSeries('group.server1.metric2',0,1,1,[None]),
            TimeSeries('group.server2.metric1',0,1,1,[None]),
            TimeSeries('group.server2.metric2',0,1,1,[None]),
        ]
        mappedResult = [
            [seriesList[0],seriesList[1]],
            [seriesList[2],seriesList[3]]
        ]
        return (seriesList,mappedResult)

    def test_mapSeries(self):
        seriesList, expectedResult = self._generate_mr_series()
        results = functions.mapSeries({}, copy.deepcopy(seriesList), 1)
        self.assertEqual(results,expectedResult)

    def test_reduceSeries(self):
        sl, inputList = self._generate_mr_series()
        expectedResult   = [
            TimeSeries('group.server1.reduce.mock',0,1,1,[None]),
            TimeSeries('group.server2.reduce.mock',0,1,1,[None])
        ]
        resultSeriesList = [TimeSeries('mock(series)',0,1,1,[None])]
        mock = MagicMock(return_value = resultSeriesList)
        with patch.dict(functions.SeriesFunctions,{ 'mock': mock }):
            results = functions.reduceSeries({}, copy.deepcopy(inputList), "mock", 2, "metric1","metric2" )
            self.assertEqual(results,expectedResult)
        self.assertEqual(mock.mock_calls, [call({},inputList[0]), call({},inputList[1])])

class TestLinregress(TestCase):

    def test_linregress_is_sane(self):
        test_data = TimeSeries('test-data', 0, 100, 2, range(0, 200, 4))

        test_context = {"startTime": datetime.datetime.fromtimestamp(0),
                        "endTime": datetime.datetime.fromtimestamp(100),
                        }
        ans = functions.linregress(test_context, [test_data])
        self.assertEqual(2, len(ans[0]))
        self.assertEqual('linregress(test-data)', ans[0].name)
        self.assertEqual(0, ans[0][0])
        self.assertEqual(200, ans[0][1])

    def test_linregress_returns_future_values(self):

        test_data = TimeSeries('test-data', 0, 100, 2, range(0, 200, 4))
        test_context = {"startTime": datetime.datetime.fromtimestamp(0),
                        "endTime": datetime.datetime.fromtimestamp(200),
                        }
        ans = functions.linregress(test_context, [test_data])
        self.assertEqual(2, len(ans[0]))
        self.assertEqual(0, ans[0][0])
        self.assertEqual(400, ans[0][1])

    def test_linregress_returns_no_series_when_amount_of_nones_is_to_high(self):
        test_data_values = range(0, 20, 4)
        test_data_values[1] = None
        test_data = TimeSeries('test-data', 0, 20, 4, test_data_values)
        test_context = {"startTime": datetime.datetime.fromtimestamp(0),
                        "endTime": datetime.datetime.fromtimestamp(20),
                        }
        ans = functions.linregress(test_context, [test_data], minValidValues=0.9)
        self.assertEqual(0, len(ans))

    def test_linregress_returns_multiple_series(self):
        test_data = [TimeSeries('test-data-one', 0, 100, 2, range(0, 200, 4)),
                     TimeSeries('test-data-two', 0, 100, 4, range(0, 200, 8))
                    ]
        test_context = {"startTime": datetime.datetime.fromtimestamp(0),
                        "endTime": datetime.datetime.fromtimestamp(200),
                        }
        ans = functions.linregress(test_context, test_data)

        self.assertEqual(2, len(ans))
        self.assertEqual(2, len(ans[0]))
        self.assertEqual(2, len(ans[1]))


class TestSixSigmaHelpers(TestCase):

    def test_replace_single_none(self):
        data = np.array([1, None, 3])
        expected = np.array([1, 2, 3])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_multiple_none(self):
        data = np.array([1, None, None, 4])
        expected = np.array([1, 2, 3, 4])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_single_none_at_beginning(self):
        data = np.array([None, 4])
        expected = np.array([4, 4])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_none_at_beginning(self):
        data = np.array([None, None, None, 4, 5])
        expected = np.array([4, 4, 4, 4, 5])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_single_none_at_end(self):
        data = np.array([5, None])
        expected = np.array([5, 5])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_none_at_end(self):
        data = np.array([4, 5, None, None, None])
        expected = np.array([4, 5, 5, 5, 5])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_all_none(self):
        data = np.array([None, None, None, None, None])
        expected = np.array([None, None, None, None, None])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_replace_single_none_alone(self):
        data = np.array([None])
        expected = np.array([None])
        functions._replace_none(data)
        npt.assert_array_equal(expected, data)

    def test_parse_factor_single_value(self):
        factor = '3'
        factor_upper, factor_lower = functions._parse_factor(factor)
        self.assertEqual(3, factor_upper)
        self.assertEqual(3, factor_lower)

    def test_parse_factor_double_value_equal(self):
        factor = '3:3'
        factor_upper, factor_lower = functions._parse_factor(factor)
        self.assertEqual(3, factor_upper)
        self.assertEqual(3, factor_lower)

    def test_parse_factor_double_value_different(self):
        factor = '3:4'
        factor_upper, factor_lower = functions._parse_factor(factor)
        self.assertEqual(4, factor_upper)
        self.assertEqual(3, factor_lower)

    def test_parse_factor_raises_exception_on_invalid_input(self):
        factor = '3:4:5'
        self.assertRaises(ValueError, functions._parse_factor, factor)

    def test_align_to_hour_works_forward(self):
        value = datetime.datetime(1970, 1, 1, 12, 0, 0, 0)
        received = functions._align_to_hour(value, 'forward')
        expected = datetime.datetime(1970, 1, 1, 13, 0, 0, 0)
        self.assertEqual(expected, received)

    def test_align_to_hour_works_backward(self):
        value = datetime.datetime(1970, 1, 1, 12, 0, 0, 0)
        received = functions._align_to_hour(value, 'backward')
        expected = datetime.datetime(1970, 1, 1, 12, 0, 0, 0)
        self.assertEqual(expected, received)

    def test_align_to_hour_works_forward_reset_sub_hour(self):
        value = datetime.datetime(1970, 1, 1, 12, 1, 2, 3)
        received = functions._align_to_hour(value, 'forward')
        expected = datetime.datetime(1970, 1, 1, 13, 0, 0, 0)
        self.assertEqual(expected, received)

    def test_align_to_hour_works_backward_reset_sub_hour(self):
        value = datetime.datetime(1970, 1, 1, 12, 1, 2, 3)
        received = functions._align_to_hour(value, 'backward')
        expected = datetime.datetime(1970, 1, 1, 12, 0, 0, 0)
        self.assertEqual(expected, received)

    def test_align_to_hour_works_forward_across_days(self):
        value = datetime.datetime(1970, 1, 1, 23, 1, 2, 3)
        received = functions._align_to_hour(value, 'forward')
        expected = datetime.datetime(1970, 1, 2, 0, 0, 0, 0)
        self.assertEqual(expected, received)

    def test_six_sigma_core_basic(self):
        values = np.array([1, 1, 1, 1])
        mean, std = functions._six_sigma_core(values, 2)
        npt.assert_array_equal([1., 1.], mean)
        npt.assert_array_equal([0., 0.], std)

    def test_six_sigma_core_different_array_lengths(self):
        values = np.array([1, 1, 1, 1, 1, 1, 1, 1])
        mean, std = functions._six_sigma_core(values, 2)
        npt.assert_array_equal([1., 1., 1., 1.], mean)
        npt.assert_array_equal([0., 0., 0., 0.], std)

    def test_six_sigma_core_change_mean_and_std(self):
        values = np.array([1, 1, 3, 3])
        mean, std = functions._six_sigma_core(values, 2)
        npt.assert_array_equal([2., 2.], mean)
        npt.assert_array_equal([1., 1.], std)


class TestSixSigma(TestCase):

    def setUp(self):
        test_data = TimeSeries('test-data', 0, 1, 1, [1])
        test_data.pathExpression = 'foo'
        test_context = {"startTime": datetime.datetime(year=2014, month=6, day=1, hour=0),
                        "endTime": datetime.datetime(year=2014, month=6, day=1, hour=2),
                        }
        self.test_data = test_data
        self.test_context = test_context

    @patch('graphite.render.functions.evaluateTarget')
    def test_sixSigma_returns_mean_upper_and_lower_band(self, evaluateTarget_mock):
        returned_test_data = TimeSeries('full-data', 0, 100, 1,
                                        np.hstack([np.arange(10) for i in range(10)]))
        evaluateTarget_mock.return_value = [returned_test_data]
        ans = functions.sixSigma(self.test_context,
                                 [self.test_data],
                                 period='2h',
                                 repeats=10,
                                 factor='3:4'
                                 )
        self.assertEqual("sixSigmaMean(%s, period='-2h', repeats=10)" % self.test_data.name, ans[0].name)
        self.assertEqual("sixSigmaUpper(%s, period='-2h', repeats=10, factor=4.0)" % self.test_data.name, ans[1].name)
        self.assertEqual("sixSigmaLower(%s, period='-2h', repeats=10, factor=3.0)" % self.test_data.name, ans[2].name)

    @patch('graphite.render.functions.evaluateTarget')
    def test_sixSigma_has_default_arguments(self, evaluateTarget_mock):
        returned_test_data = TimeSeries('full-data', 0, 100, 1, np.hstack([np.arange(7 * 24) for i in range(8)]))
        evaluateTarget_mock.return_value = [returned_test_data]
        ans = functions.sixSigma(self.test_context, [self.test_data])
        self.assertEqual("sixSigmaMean(%s, period='-7d', repeats=8)" % self.test_data.name, ans[0].name)
        self.assertEqual("sixSigmaUpper(%s, period='-7d', repeats=8, factor=3.0)" % self.test_data.name, ans[1].name)
        self.assertEqual("sixSigmaLower(%s, period='-7d', repeats=8, factor=3.0)" % self.test_data.name, ans[2].name)

    @patch('graphite.render.functions.evaluateTarget')
    def test_sixSigma_works(self, evaluateTarget_mock):
        returned_test_data = TimeSeries('full-data', 0, 100, 1,
                                        (np.hstack([np.arange(7 * 24) for i in range(8)])))
        evaluateTarget_mock.return_value = [returned_test_data]
        ans = functions.sixSigma(self.test_context, [self.test_data])
        # the mean is zero, so the upper and lower bands should equal the mean
        npt.assert_array_equal(ans[0], ans[1])
        npt.assert_array_equal(ans[0], ans[2])

    @patch('graphite.render.functions.evaluateTarget')
    def test_sixSigma_mean_upper_and_lower_band_differ_when_std_deviation_is_not_zero(self, evaluateTarget_mock):
        returned_test_data = TimeSeries('full-data', 0, 100, 1,
                                        np.concatenate(
                                            (np.arange(start=0, stop=7 * 2400, step=100),
                                             np.hstack([np.arange(7 * 24) for i in range(7)]))))
        evaluateTarget_mock.return_value = [returned_test_data]
        ans = functions.sixSigma(self.test_context, [self.test_data])
        self.assertRaises(AssertionError, npt.assert_array_equal, ans[0], ans[1])
        self.assertRaises(AssertionError, npt.assert_array_equal, ans[0], ans[2])

    @patch('graphite.render.functions.evaluateTarget')
    def test_sixSigma_has_predictable_std(self, evaluateTarget_mock):
        returned_test_data = TimeSeries('full-data', 0, 100, 1,
                                        np.concatenate([np.ones(672),
                                                        np.zeros(672)]))
        evaluateTarget_mock.return_value = [returned_test_data]
        ans = functions.sixSigma(self.test_context, [self.test_data])
        npt.assert_array_equal(np.ones(168) * 0.5, ans[0])
        npt.assert_array_equal(np.ones(168) * 2.0, ans[1])
        npt.assert_array_equal(np.ones(168) * -1.0, ans[2])
