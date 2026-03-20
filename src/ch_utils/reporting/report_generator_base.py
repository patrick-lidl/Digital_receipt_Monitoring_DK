from abc import ABC
from typing import List


class ReportGeneratorBase(ABC):
    """
    A class to generate structured HTML reports using the Arakawa library.

    Supports adding multiple sections and subsections with navigation,
    tables, plots, and descriptive text.
    """

    def __init__(
        self,
        title: str = "Report",
        sub_title: str = "<Placeholder>",
    ):
        """
        Initialize the ReportGenerator with a title.

        Args:
            title (str): The title of the report. Defaults to "Report".
            sub_title (str): The sub-title of the report. Defaults to "<Placeholder>".
        """
        import arakawa as ar

        self.title = title
        self.sub_title = sub_title
        self.sections: List = []  # NOTE: this must be an arakawa block

        self.formatting = ar.Formatting(
            light_prose=False,
            accent_color="#3392ff",  # Lidl color
            bg_color="#f2f2f2",  # Lidl color
            text_alignment=ar.TextAlignment.LEFT,
            font=ar.FontChoice.SANS,
            width=ar.Width.MEDIUM,
        )

    def save(self, path: str):
        """
        Save the constructed report to the specified file path.

        This method compiles the report header, navigation, all added sections, and a footer,
        then saves the complete report as an HTML file.

        Args:
            path (str): The file path where the report should be saved.
        """
        import arakawa as ar

        report_header = ar.HTML(
            self.create_report_html_header(title=self.title, subtitle=self.sub_title),
            label="header",
        )
        report_footer = ar.HTML(self.create_report_html_footer(footer="Daten & KI - Lidl CH"))

        view = ar.Blocks(
            report_header,
            *self.sections,
            report_footer,
        )

        ar.save_report(
            view,
            path=path,
            formatting=self.formatting,
        )

    @staticmethod
    def create_report_html_header(title: str, subtitle: str) -> str:
        """
        Create a html report header element that includes a title, subtitle, gradient and the Lidl logo.

        Args:
            title (str): Title to be displayed.
            subtitle (str): Subtitle to be displayed.

        Returns:
            An html banner.
        """
        lidl_logo = """<?xml version="1.0" encoding="utf-8"?>
                <!-- Generator: Adobe Illustrator 22.1.0, SVG Export Plug-In . SVG Version: 6.00 Build 0)  -->
                <svg width="100" height="100" version="1.1" id="Lidl_Logo" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" x="0px" y="0px"
                    viewBox="0 0 115 115" style="enable-background:new 0 0 115 115;" xml:space="preserve">
                <style type="text/css">
                    .st0{fill-rule:evenodd;clip-rule:evenodd;fill:#FFFFFF;}
                    .st1{fill-rule:evenodd;clip-rule:evenodd;fill:#0050AA;}
                    .st2{fill-rule:evenodd;clip-rule:evenodd;fill:#E60A14;}
                    .st3{fill-rule:evenodd;clip-rule:evenodd;fill:#FFF000;}
                </style>
                <g id="Lidl_x5F_Logo_x5F_115x115px_x5F_RGB">
                    <rect x="0" class="st0" width="115" height="115"/>
                    <rect x="1" y="1" class="st1" width="113.01" height="113.01"/>
                    <path class="st2" d="M57.5,3.99c-29.54,0-53.51,23.97-53.51,53.53c0,29.54,23.97,53.5,53.51,53.5c29.54,0,53.51-23.96,53.51-53.5
                        C111.01,27.96,87.04,3.99,57.5,3.99z"/>
                    <path class="st3" d="M57.5,7.38c-27.68,0-50.12,22.45-50.12,50.14c0,27.66,22.45,50.11,50.12,50.11
                        c27.67,0,50.12-22.45,50.12-50.11C107.62,29.83,85.17,7.38,57.5,7.38L57.5,7.38z"/>
                    <polygon class="st1" points="79.53,48.2 79.53,51.6 82.17,51.6 82.17,63.41 79.53,63.41 79.53,66.83 101.81,66.83 101.81,57.52
                        92.67,62.6 92.67,51.6 95.31,51.6 95.31,48.2 79.53,48.2 	"/>
                    <path class="st1" d="M70.75,48.2H55.72v3.4h2.63v11.81h-2.63v3.42h15.03C81.86,66.83,81.97,48.2,70.75,48.2z M68.64,61.02h-0.75
                        v-7.03h0.63C71.81,53.98,71.81,61.03,68.64,61.02z"/>
                    <polygon class="st2" points="54.39,58.91 45.54,50.07 35.34,60.28 35.34,63.71 37.91,61.13 45.03,68.27 42.4,70.89 44.11,72.61
                        58.35,58.35 58.35,54.93 	"/>
                    <path class="st2" d="M44.24,37.61c3.1,0,5.61,2.5,5.61,5.6c0,3.1-2.51,5.61-5.61,5.61s-5.61-2.51-5.61-5.61
                        C38.63,40.1,41.14,37.61,44.24,37.61L44.24,37.61z"/>
                    <polygon class="st1" points="13.08,48.2 28.84,48.2 28.84,51.6 26.21,51.6 26.21,62.6 35.34,57.52 35.34,66.83 13.08,66.83
                        13.08,63.41 15.72,63.41 15.72,51.6 13.08,51.6 13.08,48.2 	"/>
                </g>
                </svg>"""  # noqa: E501

        return (
            """
        <html>
            <head>
                <title>Project Banner</title>
                <style>
                    .banner {
                        background: linear-gradient(
                            to right, rgb(255,240,85) 0%, rgb(255,240,0) 60%, rgb(255,160,0) 100%
                        );
                        color: white;
                        font-size: 36px;
                        font-weight: bold;
                        text-align: left;
                        display: flex;
                        align-items: flex-start; /* add align-items */
                        justify-content: center;
                        flex-direction: column;
                        padding: 10px 20px;
                        position: relative;
                        height: 140px; /* set height to 100% */
                    }
                    .banner-title {
                        font-size: 36px;
                        font-weight: bold;
                        margin-bottom: 10px; /* add margin below title */
                        align-self: flex-start;
                        text-align: left;
                        color: #0050aa; /* change color to blue */
                        margin-right: auto; /* add margin-right */
                    }
                    .banner-subtitle {
                        font-size: 20px;
                        font-weight: bold;
                        align-self: flex-start;
                        color: #0050aa; /* add color to subtitle */
                        margin-right: auto; /* add margin-right */
                        }
                    .banner svg {
                        position: absolute;
                        top: 20px;
                        right: 20px;
                        padding: 10px;}
                </style>
            </head>
            <body>
                <div class="banner">
                    <div class="banner-title">"""
            + title
            + """</div>
                    <div class="banner-subtitle">"""
            + subtitle
            + """</div>
                """
            + lidl_logo
            + """
            </div>
        </body>

        </html>
        """
        )

    @staticmethod
    def create_report_html_footer(footer: str) -> str:
        """
        Create a html report footer element that includes a small text in the bottom left corner.

        Args:
            footer (str): Footer to be displayed.

        Returns:
            An html footer.
        """
        return (
            """<!DOCTYPE html>
        <html>
        <head>
            <style>
                body {
                        margin: 0;
                        padding: 0;
                        margin-top: 50px; /* add margin-top */
                    }
                .footer {
                    position: fixed;
                    bottom: 0;
                    left: 0;
                    background-color: #f2f2f2;
                    padding: 10px;
                    font-size: 12px;
                    text-align: left;
                    width: 100%;
                    box-sizing: border-box;
                    border-top: 1px solid #ddd;
                }
            </style>
        </head>
        <body>
            <div class="footer">
                <span>"""
            + footer
            + """</span>
            </div>
        </body>
        </html>"""
        )
