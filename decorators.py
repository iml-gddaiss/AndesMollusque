def log_results(f):
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        args[0].logger.info("%s -> %s", f.__name__, res)
        return res

    return wrapper


def validate_string(max_len=0):
    def decorator(f):
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            if max_len and len(res) <= max_len:
                args[0].logger.info(
                    "string variable is within the allowed length of VARCHAR(%s) ",
                    max_len,
                )
            else:
                args[0].logger.error(
                    "string variable is NOT within the allowed length of VARCHAR(%s) ",
                    max_len,
                )
                raise ValueError
            return res

        return wrapper

    return decorator


def validate_int(min_val: int = 0, max_val: int = 2147483647):
    """validates that the return value is an integer

    :param min_val: lower range bound (inclusive), defaults to 0
    :type min_val: int, optional
    :param max_val: upper range bound (inclusive), defaults to 2147483647 (2^31-1)
    :type max_val: int, optional
    :raises ValueError: If the test fails
    """
    def decorator(f):
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            res = int(res)
            if min_val <= res and res <= max_val:
                args[0].logger.info(
                    "int variable is within the allowed length: %s <= %s <= %s",
                    min_val,
                    res,
                    max_val,
                )
            else:
                args[0].logger.error(
                    "int variable is NOT within the allowed length: %s <= %s <= %s",
                    min_val,
                    res,
                    max_val,
                )
                raise ValueError
            return res

        return wrapper

    return decorator
