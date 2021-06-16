import os


def run_chained_tests():

    # These are not real tests and the first one still fails, expectedly.
    # This was just a proof of concept that the database bug doesn't appear any longer.
    test_cases = [
        "DataFrameFieldsTestCase.test_lebensdaten_parsen",
        "DataFrameFieldsTestCase.test_field_vornamen_vornamen_trennen",
        ]

    return_value = 0

    for idx, test in enumerate(test_cases):

        with open("tests/test_logfiles/error_logs/error_control.txt", "w") as errorfile:
            errorfile.write("")

        print("running test: ", test)
        exitcode = os.system(f"python manage.py test tests.test_hsv.{test}")

        with open(f"tests/test_logfiles/error_logs/error_control.txt", "r") as errorfile:
            errormessage = errorfile.readlines()

        if errormessage:
            for line in errormessage:
                print(line)
            return_value = 1
            break

        elif exitcode != 0:
            return_value = 1
            print(f"Exitcode is not 0 for {test}. Exitcode: {exitcode}")
            break

    return return_value


if __name__ == "__main__":
    exitcode = run_chained_tests()
    print("Tests Exited with code: ", exitcode)



