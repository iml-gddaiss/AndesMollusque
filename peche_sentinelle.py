import logging

from oracle_helper import OracleHelper


class TablePecheSentinelle:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.reference_data = OracleHelper(
            access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb"
        )

    def _assert_one(self, result):
        if len(result) == 0:
            raise ValueError("recieved no result.")

        elif len(result) > 1:
            raise ValueError("recieved more than one result.")

        elif not len(result) == 1:
            raise ValueError("Expected only one result.")

    def _assert_not_empty(self, result):
        if len(result) == 0:
            raise ValueError("recieved no result.")

    def _assert_all_equal(self, result):
        print("checking")
        if len(result) == 0:
            raise ValueError("recieved no result.")
        # case single
        # elif (len(result)==1):
        #     return
        # df = pd.DataFrame(result)
        # print(df)
        print("Not implemented")
        raise Exception
        # (df[0] ==df)

    def _hard_coded_result(self, result):
        self.logger.warning("Returning hard-coded result: %s", result)
        return result

    def _seq_result(self, result=-1):
        self.logger.warning(
            "Returning a purposfully invalid SEQ-type result: %s", result
        )
        return result

    def convert_nm_2_km(self, val: float) -> float:
        """convert nautical miles to kilometers

        :param val: value (in nautical miles) to convert
        :type val: float
        :return: converted value (in kilometers)
        :rtype: float
        """
        return val * 1.852

    def convert_knots_to_kph(self, val: float) -> float:
        """convert knots to kilometers per hour

        :param val: value (in knots) to convert
        :type val: float
        :return: converted value (in km/h)
        :rtype: float
        """
        return self.convert_nm_2_km(val)

    def validate(self):
        """
        Need to override by child class
        """
        raise NotImplementedError
