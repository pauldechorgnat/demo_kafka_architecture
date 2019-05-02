import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import numpy as np
import plotly.graph_objs as go
import random
import pandas as pd
import json
import re
from collections import Counter
from happybase import Connection

# https://github.com/Sentdex/socialsentiment

# defining colors for the app
app_colors = {
    'background': '#0C0F0A',
    'text': '#FFFFFF',
    'sentiment-plot': '#41EAD4',
    'volume-bar': '#FBFC74',
    'some-other-color': '#FF206E',
}

# colors for datascientest
colors_dst = {
    'green': '#3ce9b9',
    'grey': '#484a69'
}

sentiment_color_dst = {
    'green': '#3ce9b9',
    'red': '#e93c6c'
}

# app_colors['background'] = '#484a69'
app_colors['title'] = '#3ce9b9'


# app_colors['volume-bar'] = '#e9b93c'


# defining colors for the sentiment in the tweet table
def sentiment_colors(x):
    if x < -1:
        return "#EE6055"
    elif x < -0.5:
        return "#FDE74C"
    elif x < 0:
        return "#FFE6AC"
    elif x < 0.5:
        return "#D0F2DF"
    else:
        return "#9CEC5B"


# sentiment_colors = {-1: "#EE6055",
#                     -0.5: "#FDE74C",
#                     0: "#FFE6AC",
#                     0.5: "#D0F2DF",
#                     1: "#9CEC5B", }


# building the app
app = dash.Dash(__name__)
server = app.server

# building app layout
app.layout = html.Div(
    [
        html.Div(className='container-fluid',
                 children=[
                     html.H2('Live Twitter Sentiment',
                             style={'color': app_colors['title'], 'text-align': 'center'}),
                 ],
                 style={'width': '98%', 'margin-left': 10, 'margin-right': 10, 'max-width': 50000}),
        html.Div(className='row',
                 children=[
                     html.Div(id='short-term-word-count',
                              className='col s12 m6 l6',
                              style={"word-wrap": "break-word"}),
                     html.Div(id='long-term-word-count',
                              className='col s12 m6 l6',
                              style={"word-wrap": "break-word"})
                 ]
                 ),
        html.Div(className='row',
                 children=[
                     html.Div(
                         dcc.Graph(id='short-term-history-graph',
                                   animate=False),
                         className='col s12 m6 l6'),
                     html.Div(
                         dcc.Graph(id='long-term-history-graph',
                                   animate=False),
                         className='col s12 m6 l6')
                 ]
                 ),
        html.Div(className='row',
                 children=[
                     html.Div(id="short-term-tweets-table",
                              className='col s12 m6 l6'),
                     html.Div(
                         dcc.Graph(id='long-term-sentiment-pie',
                                   animate=False),
                         className='col s12 m6 l6'),
                 ]
                 ),
        dcc.Interval(
            id='short-term-update',
            interval=2 * 1000
        ),
        dcc.Interval(
            id='long-term-update',
            interval=10 * 1000
        ),
    ],
    style={
        'backgroundColor': app_colors['background'],
        'margin-top': '-30px',
        'height': '2000px',
    },
)


# defining functions to get data from H BASE
def get_data_for_short_term_word_count():
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='word_count_tweets_table')
    data = table.row(row='latest')
    data = {w.decode('utf-8').split(':')[1]: int(c) for w, c in data.items()}
    return data


def get_data_for_long_term_word_count(number_of_intervals=5):
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='word_count_tweets_table')
    data = table.scan()
    total_data = {}
    for index, (row_id, word_count) in enumerate(data):
        if index == number_of_intervals:
            break
        for raw_word, count in word_count.items():
            word = raw_word.decode('utf-8').split(':')[1]
            total_data[word] = total_data.get(word, 0) + int(count)

    return total_data


def get_data_for_short_term_history(number_of_intervals=15):
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='predictions_tweets_table')
    data = table.scan()
    total_data = {'volume': [], 'positive': [], 'negative': []}
    for index, (row_id, predictions) in enumerate(data):
        if index == number_of_intervals:
            break
        positive_predictions = int(predictions.get('tweet_count:positive'.encode('utf-8'), 0))
        negative_predictions = int(predictions.get('tweet_count:negative'.encode('utf-8'), 0))
        volume = positive_predictions + negative_predictions
        total_data['volume'].append(volume)
        total_data['positive'].append(positive_predictions)
        total_data['negative'].append(negative_predictions)

    return total_data


def get_data_for_long_term_history(number_of_intervals=60):
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='predictions_tweets_table')
    data = table.scan()
    total_data = {'volume': [], 'positive': [], 'negative': []}
    for index, (row_id, predictions) in enumerate(data):
        if index == number_of_intervals:
            break
        positive_predictions = int(predictions.get('tweet_count:positive'.encode('utf-8'), 0))
        negative_predictions = int(predictions.get('tweet_count:negative'.encode('utf-8'), 0))
        volume = positive_predictions + negative_predictions
        total_data['volume'].append(volume)
        total_data['positive'].append(positive_predictions)
        total_data['negative'].append(negative_predictions)

    return total_data


def get_data_for_pie_chart(number_of_intervals=60):
    data = get_data_for_long_term_history(number_of_intervals=number_of_intervals)
    data['volume'] = sum(data['volume'])
    data['positive'] = sum(data['positive'])
    data['negative'] = sum(data['negative'])
    return data


def get_data_for_tweet_table(number_of_tweets=5):
    connection = Connection(host='localhost', port=9090, autoconnect=True)
    table = connection.table(name='text_tweets_table')
    data = table.scan()
    total_data = {'time': [], 'text': [], 'authors': []}
    for index, (row_id, tweet_data) in enumerate(data):
        if index == number_of_tweets:
            break
        total_data['time'].append(tweet_data.get('metadata:publication_date'.encode('utf-8'))[:19])
        total_data['authors'].append(tweet_data.get('metadata:author'.encode('utf-8')))
        total_data['text'].append(tweet_data.get('text_data:text'.encode('utf-8')))

    return pd.DataFrame(total_data)


# callbacks for short term updates
# updating the recent word count
@app.callback(Output('short-term-word-count', 'children'),
              [Input('short-term-update', 'n_intervals')])
def update_short_term_word_count(n):
    word_count = get_data_for_short_term_word_count()
    word_count.pop('trump')
    sizes = list(word_count.values())
    sentiments = {word: random.uniform(-1, 1) for word in word_count}

    size_min = min(sizes)
    size_max = max(sizes) - size_min + 1

    buttons = [html.H5('Short-term word count: ', style={'color': app_colors['text']})] + \
              [html.Span(word,
                         style={'color': sentiment_colors(round(sentiments[word] * 2) / 2),
                                'margin-right': '15px',
                                'margin-top': '15px',
                                'font-size': '{}%'.format(generate_size(word_count[word], size_min, size_max))})
               for
               word in word_count]

    return buttons


# updating the short term history graph of the sentiments
@app.callback(Output('short-term-history-graph', 'figure'),
              [Input('short-term-update', 'n_intervals')])
def update_short_term_history_graph(n):
    data_from_twitter = get_data_for_short_term_history()
    y_positive = list(reversed(data_from_twitter['positive']))
    y_negative = list(reversed(data_from_twitter['negative']))
    y_volume = list(reversed(data_from_twitter['volume']))
    x = np.linspace(start=1, stop=len(y_positive), num=len(y_positive))
    data_positive = go.Scatter(
        x=x,
        y=y_positive,
        name='Positive',
        mode='lines',
        yaxis='y2',
        line=dict(color=sentiment_color_dst['green'],
                  width=4, )
    )
    data_negative = go.Scatter(
        x=x,
        y=y_negative,
        name='Negative',
        mode='lines',
        yaxis='y2',
        line=dict(color=sentiment_color_dst['red'],
                  width=4, )
    )

    data_volume = go.Bar(
        x=x,
        y=y_volume,
        name='Volume',
        yaxis='y',
        marker=dict(color=app_colors['volume-bar']),
    )

    return {'data': [data_positive, data_negative, data_volume],
            'layout': go.Layout(xaxis=dict(range=[min(x), max(x)]),
                                yaxis=dict(range=[
                                    min(y_volume),
                                    max(y_volume)],
                                    title='Volume',
                                    side='right'),
                                yaxis2=dict(
                                    range=[min(min(y_negative),
                                               min(y_positive)),
                                           max(max(y_negative),
                                               max(y_positive))],
                                    side='left',
                                    overlaying='y',
                                    title='sentiment'),
                                title='Live sentiment',
                                font={'color': app_colors['text']},
                                plot_bgcolor=app_colors[
                                    'background'],
                                paper_bgcolor=app_colors[
                                    'background'],
                                showlegend=False)}


# updating the table containing the recent tweets
@app.callback(Output('short-term-tweets-table', 'children'),
              [Input('short-term-update', 'n_intervals')])
def update_short_term_tweets_table(n):
    df = get_data_for_tweet_table(5)
    df.columns = ['Time', 'Tweet', 'Author']
    for col in df.columns:
        df[col] = df[col].map(lambda x: x.decode('utf-8'))
    # print(df.head())
    return generate_table(df)


# functions used to create the tables
# defining a function to color text with respect to the sentiment
def quick_color(s, pos_neg_neut=0.1):
    # except return bg as app_colors['background']
    if s >= pos_neg_neut:
        # positive
        return "#002C0D"
    elif s <= -pos_neg_neut:
        # negative:
        return "#270000"

    else:
        return app_colors['background']


# defining a function to return a HTML table containing tweets
def generate_table(df, max_rows=5):
    return html.Table(className="responsive-table",
                      children=[
                          html.Thead(
                              html.Tr(
                                  children=[
                                      html.Th(col.title()) for col in df.columns.values],
                                  style={'color': app_colors['text']}
                              )
                          ),
                          html.Tbody(
                              [

                                  html.Tr(
                                      children=[
                                          html.Td(data) for data in d
                                      ], style={'color': app_colors['text'],
                                                'background-color': quick_color(0)}
                                  )
                                  for d in df.values.tolist()[:max_rows]])
                      ]
                      )


# defining a function to generate the size of the related terms
def generate_size(value, size_min, size_max, max_size_change=.4):
    size_change = round((((value - size_min) / size_max) * 2) - 1, 2)
    final_size = (size_change * max_size_change) + 1
    return final_size * 120


# updating long term charts
# updating the recently trading mentions
@app.callback(Output('long-term-word-count', 'children'),
              [Input(component_id='long-term-update', component_property='n_intervals')])
def update_long_term_word_count(n):
    word_count = get_data_for_long_term_word_count()
    word_count.pop('trump')
    sizes = list(word_count.values())

    size_min = min(sizes)
    size_max = max(sizes) - size_min + 1

    normalized_word_count = {w: float(word_count[w] - size_min) / float(size_max - size_min) - 0.49 for w in
                             sorted(word_count,
                                    reverse=True)[:100]}

    buttons = [html.H5('Long-term word count: ', style={'color': app_colors['text']})] + \
              [html.Span(word,
                         style={'color': sentiment_colors(round(normalized_word_count[word] * 2) / 2),
                                'margin-right': '15px',
                                'margin-top': '15px',
                                'font-size': '{}%'.format(generate_size(word_count[word], size_min, size_max))})
               for
               word in normalized_word_count]

    return buttons


# updating pie chart
# @app.callback(Output('long-term-sentiment-pie', 'figure'),
#               [Input('long-term-update', 'n_intervals')])
# def update_long_term_sentiment_pie(n):
#     data = get_data_for_pie_chart()
#     labels = ['Positive', 'Negative']
#     pos = data['positive']
#     neg = data['negative']
#     values = [pos, neg]
#     colors = [sentiment_color_dst['green'], sentiment_color_dst['red']]
#     trace = go.Pie(labels=labels, values=values,
#                    hoverinfo='label+percent', textinfo='value',
#                    textfont=dict(size=20, color=app_colors['text']),
#                    marker=dict(colors=colors,
#                                line=dict(color=app_colors['background'], width=2)))
#
#     return {"data": [trace], 'layout': go.Layout(
#         title='Positive vs Negative sentiment" (longer-term)',
#         font={'color': app_colors['text']},
#         plot_bgcolor=app_colors['background'],
#         paper_bgcolor=app_colors['background'],
#         showlegend=True)}


# updating historical graph
@app.callback([Output('long-term-history-graph', 'figure'), Output('long-term-sentiment-pie', 'figure')],
              [Input('long-term-update', 'n_intervals')])
def update_long_term_history_graph(n):
    data_from_twitter = get_data_for_long_term_history()
    y_positive = list(reversed(data_from_twitter['positive']))
    y_negative = list(reversed(data_from_twitter['negative']))
    y_volume = list(reversed(data_from_twitter['volume']))
    x = np.linspace(start=1, stop=len(y_positive), num=len(y_positive))
    data_positive = go.Scatter(
        x=x,
        y=y_positive,
        name='Positive',
        mode='lines',
        yaxis='y2',
        line=dict(color=sentiment_color_dst['green'],
                  width=4, )
    )
    data_negative = go.Scatter(
        x=x,
        y=y_negative,
        name='Negative',
        mode='lines',
        yaxis='y2',
        line=dict(color=sentiment_color_dst['red'],
                  width=4, )
    )

    data_volume = go.Bar(
        x=x,
        y=y_volume,
        name='Volume',
        yaxis='y',
        marker=dict(color=app_colors['volume-bar']),
    )

    labels = ['Positive', 'Negative']
    pos = sum(y_positive)
    neg = sum(y_negative)
    values = [pos, neg]
    colors = [sentiment_color_dst['green'], sentiment_color_dst['red']]
    trace = go.Pie(labels=labels, values=values,
                   hoverinfo='label+percent', textinfo='value',
                   textfont=dict(size=20, color=app_colors['text']),
                   marker=dict(colors=colors,
                               line=dict(color=app_colors['background'], width=2)))

    return ({'data': [data_positive, data_negative, data_volume],
             'layout': go.Layout(xaxis=dict(range=[min(x), max(x)]),
                                 yaxis=dict(range=[
                                     min(y_volume),
                                     max(y_volume)],
                                     title='Volume',
                                     side='right'),
                                 yaxis2=dict(
                                     range=[min(min(y_negative),
                                                min(y_positive)),
                                            max(max(y_negative),
                                                max(y_positive))],
                                     side='left',
                                     overlaying='y',
                                     title='sentiment'),
                                 title='Sentiment history',
                                 font={'color': app_colors['text']},
                                 plot_bgcolor=app_colors[
                                     'background'],
                                 paper_bgcolor=app_colors[
                                     'background'],
                                 showlegend=False)},

            {"data": [trace], 'layout': go.Layout(
                title='Positive vs Negative sentiment" (longer-term)',
                font={'color': app_colors['text']},
                plot_bgcolor=app_colors['background'],
                paper_bgcolor=app_colors['background'],
                showlegend=True)})


# adding external css and js style sheets
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css"]
for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ['https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/js/materialize.min.js',
               'https://pythonprogramming.net/static/socialsentiment/googleanalytics.js']
for js in external_js:
    app.scripts.append_script({'external_url': js})

if __name__ == '__main__':
    app.run_server()

