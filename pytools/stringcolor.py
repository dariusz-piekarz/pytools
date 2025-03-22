def is_in_interval(n: int, lower: int, upper: int) -> bool:
    """
    Checks if n is between lower and upper.

    :param n: int an integer number
    :param lower:  int a lower bound
    :param upper:  in an upper bound
    :return: bool
    """

    if not isinstance(n, int):
        raise ValueError(f"`n` must be integer. Got instead {type(n)}.")

    return lower <= n <= upper


class Color:
    """
    Class that colors string representation (font and bg) of passed object. There is possible to
    select the color from prescribed colors, or using RGB.
    """

    font: dict[str, str]
    bg: dict[str, str]
    reset_font: str
    reset_bg: str

    def __init__(self) -> None:
        self.set_params()

    def set_params(self) -> None:
        self.font = {
            "white": "\033[37m",
            "purple": "\033[35m",
            "cyan": "\033[36m",
            "black": "\033[30m",
            "red": "\033[31m",
            "blue": "\033[34m",
            "green": "\033[32m",
            "yellow": "\033[33m",
            "gray": "\033[90m",
            "lightwhite": "\033[97m",
            "lightred": "\033[91m",
            "lightblue": "\033[94m",
            "lightgreen": "\033[92m",
            "lightyellow": "\033[93m",
            "lightpurple": "\033[95m",
            "lightcyan": "\033[96m",
        }

        self.bg = {
            "white": "\033[47m",
            "black": "\033[40m",
            "red": "\033[41m",
            "blue": "\033[44m",
            "green": "\033[42m",
            "yellow": "\033[43m",
            "purple": "\033[45m",
            "cyan": "\033[46m",
        }

        self.reset_font = "\033[39m"
        self.reset_bg = "\033[49m"

    def paint(self, to_color: any, font_color: str = "red", bg_color: str = "black") -> str:
        """
        Paint `to_color` string representation with font `font_color` color and set the background color to `bg_color`.
        Colors are presciribed.

        :param to_color:  object to be colored
        :param font_color: str representation of the font color
        :param bg_color: str representation of bg color
        :return: str colored string
        """
        if font_color not in self.font.keys():
            raise ValueError(f"`font_color` color {font_color} not supported. Select one of {list(self.font.keys())}")

        if bg_color not in self.bg.keys():
            raise ValueError(f"`bg_color` color {bg_color} not supported. Select one of {list(self.bg.keys())}")

        return f"{self.font[font_color]}{self.bg[bg_color]}{str(to_color)}{self.reset_font}{self.reset_bg}"

    def paint_rgb(
        self,
        to_color: str,
        font_rgb: tuple[int, int, int],
        bg_rgb: tuple[int, int, int],
    ) -> str:
        """
        Paint `to_color` string representation (font and bg) by provided RGB
        :param to_color: any - object to paint
        :param font_rgb: Tuple[int, int, int] - font color in RGB form
        :param bg_rgb: Tuple[int, int, int] - bg color in RGB form
        :return: str - colored string
        """

        if not all([is_in_interval(rgb, 0, 255) for rgb in font_rgb]):
            raise ValueError(f"`font_rgb` must be 3-tuple of integer numbers in range [0, 255]. Got {font_rgb}.")

        if not all([is_in_interval(rgb, 0, 255) for rgb in bg_rgb]):
            raise ValueError(f"`font_rgb` must be 3-tuple of integer numbers in range [0, 255]. Got {bg_rgb}.")

        font_color = f"\033[38;2;{font_rgb[0]};{font_rgb[1]};{font_rgb[2]}m"
        bg_color = f"\033[48;2;{bg_rgb[0]};{bg_rgb[1]};{bg_rgb[2]}m"

        return f"{font_color}{bg_color}{str(to_color)}{self.reset_font}{self.reset_bg}"
