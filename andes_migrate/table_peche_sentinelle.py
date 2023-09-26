import logging

from andes_migrate.oracle_helper import OracleHelper


class TablePecheSentinelle:
    """Abstract Class
    This acts like a parent class to provide basic functionality
    for objects representing a Peche Sentinelle table.


    """

    def __init__(self, ref: OracleHelper | None = None):
        self.logger = logging.getLogger(__name__)
        # self.reference_data = OracleHelper(
        #     access_file="./Relevés_Pétoncle_Globale_juin2020_PG .mdb"
        # )
        if ref:
            self.reference_data = ref
        else:
            self.reference_data = OracleHelper()

        self._row_list = []
        self._row_idx: int | None = None

    def _init_rows(self):
        """Initialisation method
        This queries the Andes DB and creaters a list of row entries to be added to the current table

        After running this methods initialises the following attribute:
        self._row_list
        self._row_idx (hopefully to self._row_idx=0)

        Need to override by child class
        """
        raise NotImplementedError

    def _get_current_row_pk(self) -> int:
        """
        Return the Andes primary key of the current row
        """
        if self._row_idx is not None and self._row_list:
            return self._row_list[self._row_idx]
        else:
            raise ValueError

    def _increment_row(self):
        """
        Increment to focus on next row
        """
        if self._row_idx and self._row_list:
            if self._row_idx < len(self._row_list) - 1:
                self._row_idx += 1
            else:
                raise StopIteration
        else:
            self.logger.error("Row data not initialise, did you run _init_rows()?")
            raise ValueError

    def _assert_one(self, result):
        """asserts that the query returns only one result.
        raises ValueError otherwise
        """
        if len(result) == 0:
            raise ValueError("recieved no result.")

        elif len(result) > 1:
            raise ValueError("recieved more than one result.")

        elif not len(result) == 1:
            raise ValueError("Expected only one result.")

    def _assert_not_empty(self, result):
        """asserts that the query result is not empty,
        raises ValueError otherwise
        """
        if len(result) == 0:
            raise ValueError("recieved no result.")

    def _assert_all_equal(self, result):
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
        """returns a value hard-coded value
        Some values cannot be looked up and have to be hard-coded into the module.
        This function simply returns back the input value, but also prints
        a logging warning.
        """
        self.logger.warning("Returning hard-coded result: %s", result)
        return result

    def _seq_result(self, result=-1):
        """returns a value meant to be a sequential value
        As SEQ values are not meant to be populated, this function
        simply prints a warning before returning the input.
        """
        self.logger.warning(
            "Returning a purposfully invalid SEQ-type result: %s", result
        )
        return result

    def validate(self):
        """
        This executes a battery of validation tests to help
        find errors.

        Need to override by child class
        """
        raise NotImplementedError
