from functools import wraps


class Tag:
    pass


class AndesCodeLookup(Tag):
    """used to tag as a code lookup match (from andes)"""

    pass


class HardCoded(Tag):
    """used to tag as a returning a hard-coded value"""

    pass


class Computed(Tag):
    """used to tag as a computed value"""

    pass


class NotAndes(Tag):
    """used to tag as not recorded in Andes"""

    pass


class Deprecated(Tag):
    """used to tag as deprecated"""

    pass

class FixMe(Tag):
    """used to tag as code that needs to be fixed"""

    pass


class Seq(Tag):
    """used to tag as a SEQ-type"""

    pass


def tag(*tags):
    """Decorator to add tags to a method."""

    def decorator(obj):
        if hasattr(obj, "tags"):
            obj.tags = obj.tags.union(tags)
        else:
            setattr(obj, "tags", set(tags))
        return obj

    return decorator


def deprecate(successor=None):
    """Decorator to deprecate methods."""

    def decorator(f):
        # apply deprecated tag
        if hasattr(f, "tags"):
            f.tags = f.tags.add(Deprecated)
        else:
            setattr(f, "tags", set([Deprecated]))

        @wraps(f)
        def wrapper(*args, **kwargs):
            if successor:
                args[0].logger.warn(
                    "The function is deprecated, please use %s", successor
                )
            else:
                args[0].logger.warn(
                    "The function is tied to a column that should be deprecated."
                )
            return f(*args, **kwargs)

        return wrapper

    return decorator


def log_results(f):
    """Decorator to log activity."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        args[0].logger.info("%s -> %s", f.__name__, res)
        return res

    return wrapper


def validate_string(max_len: int = 255, not_null: bool = True):
    """Decorator to validate string length and type.
    :param max_len: max string length (inclusive), defaults to 255
    :type min_val: int, optional
    :param not_null: test if value is forbidden from being null/None
    :type max_val: bool, optional
    :raises ValueError: If the test fails

    """

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            if res is None and not_null:
                args[0].logger.info("Value cannot be null")
                raise ValueError
            elif res is None:
                args[0].logger.info("String variable is Null and allowed")
                return res

            if max_len and len(res) <= max_len:
                args[0].logger.info(
                    "String variable is within the allowed length of VARCHAR(%s) ",
                    max_len,
                )
            else:
                args[0].logger.error(
                    "String variable is NOT within the allowed length of VARCHAR(%s) ",
                    max_len,
                )
                raise ValueError
            return res

        return wrapper

    return decorator


def validate_int(min_val: int = 0, max_val: int = 2147483647, not_null: bool = True):
    """Decorator to validate integers

    :param min_val: lower range bound (inclusive), defaults to 0
    :type min_val: int, optional
    :param max_val: upper range bound (inclusive), defaults to 2147483647 (2^31-1)
    :type max_val: int, optional
    :param not_null: test if value is forbidden from being null/None
    :type max_val: bool, optional
    :raises ValueError: If the test fails
    """

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            res = f(*args, **kwargs)
            if res is None and not_null:
                args[0].logger.info("Value cannot be null")
                raise ValueError
            elif res is None:
                args[0].logger.info("Value is Null and allowed")
                return res

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
