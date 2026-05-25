"""Helper functions and strings."""

import click

EXTRACT_DESCRIPTION = "Extract the provided child file (must be a trusted archive)"
EXTRACT_PASSWORD_DESCRIPTION = "Password for the extracted archive to extracted with."  # noqa S105
FULL_INFO = "Show full configuration."
NO_CONFIRMATION_PROMPT = "Skip confirmation prompt."
REFERENCES = "References for sample. Accepts multiple arguments. e.g. --ref user:llama --ref location:ocean"
SECURITY_STRING_DESCRIPTION = "simple security string (use `azul security` to see available security strings)"
TIMESTAMP_DESCRIPTION = (
    "Timestamp for which the file being submitted was sourced in ISO8601 format e.g 2025-05-26T02:11:44Z"
)


class ExamplesCommand(click.Command):
    """Passed through to cls in click.command(cls=ExamplesCommand)."""

    def format_help(self, ctx, formatter):  # noqa D102
        super().format_help(ctx, formatter)
        examples = getattr(self.callback, "__examples__", ())
        if examples:
            with formatter.section("Examples"):
                for example in examples:
                    formatter.write_text(example)


def with_examples(*examples):
    """Used to pass Example strings to click commands as a decorator."""

    def decorator(func):  # noqa D102
        func.__examples__ = examples

        def make_command(name, **attrs):
            cmd = ExamplesCommand(name, callback=func, **attrs)
            # cmd.examples = getattr(func, "__examples__", [])
            return cmd

        func.__click_params__ = getattr(func, "__click_params__", [])
        func.__make_command__ = make_command
        return func

    return decorator
