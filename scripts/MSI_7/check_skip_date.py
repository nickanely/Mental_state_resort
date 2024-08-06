DATES_TO_SKIP = ["11.06", "13.06", "15.06", "25.06", ]


def check_skip_date(**kwargs):
    execution_date = kwargs["execution_date"].format("DD.MM")
    if execution_date in DATES_TO_SKIP:
        return "skip_message"
    else:
        return "send_to_teams"
