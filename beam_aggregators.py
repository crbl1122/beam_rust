import apache_beam as beam
import float_sum  # Import the pyo3 package

# class ComputeAverage(beam.CombineFn):
#     def create_accumulator(self):
#         return (0.0, 0)  # (sum, count)

#     def add_input(self, accumulator, input):
#         (sum_, count) = accumulator
#         return sum_ + input, count + 1

#     def merge_accumulators(self, accumulators):
#         sum_, count = 0.0, 0
#         for (sum_, count) in accumulators:
#             sum_ = float_sum.sum_as_float(sum_, sum_)
#             count = float_sum.sum_as_float(count, count)
#         return sum_, count

#     def extract_output(self, accumulator):
#         (sum_, count) = accumulator
#         return sum_ / count if count else float('NaN')


class ComputeAverage(beam.CombineFn):
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, input):
        if not accumulator:
            # Initialize the accumulator with zeros for each field
            accumulator.update({field: {'sum': 0.0, 'count': 0} for field in input if isinstance(input[field], (int, float))})
        
        for field in accumulator:
            if field in input:
                if isinstance(input[field], (int, float)):
                    accumulator[field]['sum'] = float_sum.sum_as_float(accumulator[field]['sum'], input[field])
                    accumulator[field]['count'] = float_sum.sum_as_float(accumulator[field]['count'], 1.0)
        
        return accumulator

    def merge_accumulators(self, accumulators):
        if not accumulators:
            return {}

        merged_accumulator = {}

        for acc in accumulators:
            for field, stats in acc.items():
                if field not in merged_accumulator:
                    merged_accumulator[field] = {'sum': 0.0, 'count': 0}
                merged_accumulator[field]['sum'] = float_sum.sum_as_float(merged_accumulator[field]['sum'], stats['sum'])
                merged_accumulator[field]['count'] += stats['count']

        return merged_accumulator

    def extract_output(self, accumulator):
        averages = {}

        for field, stats in accumulator.items():
            if stats['count'] > 0:
                averages[field] = stats['sum'] / stats['count']
            else:
                averages[field] = float('NaN')

        return averages
