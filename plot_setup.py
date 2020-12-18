from colour import Color
import altair as alt

def finastra_theme():

    ###### COLORS ######
    violet = "#694ED6"
    fuchsia = "#C137A2"
    white = "#FFFFFF"

    violet_c = Color(violet)
    fuchsia_c = Color(fuchsia)
    white_c = Color(white)

    cont = [str(c) for c in fuchsia_c.range_to(violet_c, 50)]
    div = [str(c) for c in fuchsia_c.range_to(white_c, 25)] + [str(c) for c in
           white_c.range_to(violet_c, 25)]
    disc = ["#C137A2", "#694ED6", "#26D07C", "#009CBD", "#F04E98",
            "#ED8B00", "#FFD100", "#F9423A", "#C7C8CA", "#414141"]


    ###### TEXT ######
    title_font = "Futura"
    title_font_size = 22
    tick_font = "Roboto"
    tick_font_size = 12
    text_font = "Roboto"
    text_font_size = 14

    ###### CONFIGURATION ######
    config = {"config": {
        
            "background": '#FFFFFF',

            "title": {
                "anchor": "middle",
                "font": title_font,
                "fontSize": title_font_size
            },

            # Chart Types
            "arc": {
                "fill": violet,
                "tooltip": True},
            "area": {
                "stroke": fuchsia,
                "fill": violet,
                "tooltip": True},
            "line": {
                "stroke": fuchsia,
                "point": violet,
                "stroke_width": 3,
                "tooltip": True},
            "path": {
                "stroke": fuchsia,
                "tooltip": True},
            "rect": {
                "fill": violet,
                "tooltip": True},
            "shape": {
                "stroke": fuchsia,
                "tooltip": True},
            "symbol": {
                "fill": violet,
                "size": 30,
                "tooltip": True},

            "axis": {
                "domainWidth": 0.5,
                "grid": False,
                "labelPadding": 2,
                "tickSize": 5,
                "tickWidth": 0.5,
                "titleFontWeight": 'normal',
                "titleFont": text_font,
                "titleFontSize": text_font_size,
                "labelFont": tick_font,
                "labelFontSize": tick_font_size,
            },

            "axisBand": {
                "grid": False,
            },

            "legend": {
                "labelFontSize": 12,
                "padding": 1,
                "titleFont": text_font,
                "titleFontSize": text_font_size,
                "labelFont": tick_font,
                "labelFontSize": tick_font_size,
            },

            "range": {
                "category": disc,
                "diverging": div,
                "heatmap": cont,
                "ordinal": cont,
                "ramp": cont,
            },
        }
    }

    return config
