from datetime import datetime, timedelta
import logging
from zoneinfo import ZoneInfo

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

        self.table_name: str
        self.data = {}
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
        if self._row_idx is not None and self._row_list:
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

    def get_insert_statement(self):
        col_str = [k for k in self.data.keys()]
        col_str = ", ".join(col_str)
        col_str = " (" + col_str + ") "

        val_str = [
            str(self.reference_data.value_2_string(k)) for k in self.data.values()
        ]
        val_str = ", ".join(val_str)
        val_str = " (" + val_str + ") "

        statement = f" INSERT INTO {self.table_name} {col_str} VALUES {val_str}"
        return statement
    

    @staticmethod
    def format_time(dt:datetime) -> tuple[str,str,bool]:
        """Format to the standard Oracle time format

        The input is a UTC datetime object, and the output is a string representation of the datetime
        in America/Montreal timezone DST.

        America/Montreal DST is the historical format foound in the Access tables.

        The following methods ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_cod_fuseau_horaire`
        and ::func:`~andes_migrate.trait_mollusque.TraitMollusque.get_cod_typ_heure`
        are hard-coded for America/Montreal DST (which is usualy the case)

        All datetime fields are processed by this helper method in case the standard format changes.

        This funciton returns a tuple:
        (datetime_str: str, timezone_str: str, is_dst:bool)

        """
        timezone_str = "America/Montreal"
        strfmt = "%Y-%m-%d %H:%M:%S"

        # daylight offset (either 1:00:00 or 00:00:00 for Quebec)         
        dst = dt.astimezone(ZoneInfo(timezone_str)).dst()
        # is_dst = dst==timedelta(hours=1)
        if dst==timedelta(hours=1):
            is_dst = True
        elif dst==timedelta(hours=0):
            is_dst = False
        else:
            raise ValueError("Cannot determine daylight saving time")  

        dt_str = dt.astimezone(ZoneInfo(timezone_str)).strftime(strfmt)

        return (dt_str, timezone_str, is_dst)

