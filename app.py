import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
from streamlit_extras.metric_cards import style_metric_cards
import pandas as pd
import plotly.express as px
import datetime
import time
import os

st.set_page_config(layout="wide")
# @title tools

df_accs = pd.read_excel('./core/accounts.xlsb').astype(str)
df_cur = pd.read_excel('./core/currency.xlsb')

DATA_DIR = "./core/data/"
COLUMNS ={'№'           :'id',
         'ОСТАТОК СУМ'  :'balance',
         'ДАТА'         :'date',
         'ПРИХОД'       :'t_pos',
         'РАСХОД'       :'t_neg',
         'Кол-во'       :'qty',
         'Цена'         :'price',
         'ОПИСАНИЕ'     :'description',
         'ВАЛЮТА'       :'currency',
         'Тип'          :'type',
         'Категория'    :'item',
         'ПодКатегория' :'subitem',
         'ХИСОБ.КИТОБ'  :'t_balance',
         'КАРТА?'       :'is_card',
         'Доп'          :'comment'}

COLUMNS_TO_SHOW ={
                'date'        :  'ДАТА',
                't_pos'       :  'ПРИХОД',
                't_neg'       :  'РАСХОД',
                'description' :  'ОПИСАНИЕ',
                'account'     :  'Счет',
                'currency'    :  'ВАЛЮТА',
                'type'        :  'Тип',
                'item'        :  'Категория',
                'subitem'     :  'ПодКатегория',
                'comment'     :  'Доп'}

COLUMNS_TO_DROP = ['id','qty','price', 'balance','is_card']

TO_ACCOUNTS ={'ДОЛ' :'USD',
             'ЕВРО':'EUR',
             '0'   :'UZS_cash',
             'КЛИК':'UZS_click',
             'РС'  :'UZS_bank',
             'РУБ' :'RUB'}

ACCOUNTS_TO_CURRENCIES = df_accs.set_index('account_id').to_dict()['currency']

DATE_GRAN_DICT = {'день':'D','неделя':'W','месяц':'M'} 

def convert_to_uzs(df,df_cur):
    df     = df.copy()
    df_cur = df_cur.copy()

    df = pd.merge_asof(df.sort_values('date'), 
                            df_cur.sort_values('date').query(f'`to` == "UZS"').rename(columns={'from':'currency'}),
                            on='date', by='currency', direction='backward')
    df['t_pos']             = df['t_pos']*df['rate']
    df['t_neg']             = df['t_neg']*df['rate']
    # df['t_balance_orinal']  = df['t_balance'] 
    df['t_balance']         = df['t_balance']*df['rate']
    curr_change = df[['date','to','currency','rate']]
    df['currency']          = 'UZS'
    df = df.drop(columns=['to','rate','rate_inv'])
    return df, curr_change
   
def convert_to_usd(df,df_cur):
    df     = df.copy()
    df_cur = df_cur.copy()

    df = pd.merge_asof(df.sort_values('date'), 
                            df_cur.sort_values('date').query(f'`from` == "USD"').query(f'`to` == "UZS"').rename(columns={'to':'currency'}),
                            on='date', by='currency', direction='backward')
    df['t_pos']             = df['t_pos']*df['rate_inv']
    df['t_neg']             = df['t_neg']*df['rate_inv']
    # df['t_balance_orinal']  = df['t_balance'] 
    df['t_balance']         = df['t_balance']*df['rate_inv']
    df = df.drop(columns=['from','rate','rate_inv'])
    return df

@st.cache_data      
def preprocess_df(df, df_cur,base_currency='USD'):
    df = df.copy().reset_index(drop=True)
    df_cur = df_cur.copy()


    df = df.rename(columns = COLUMNS)
    df = df.drop(columns=COLUMNS_TO_DROP)
    df['date'] = pd.to_datetime(df['date'], origin='1899-12-31', unit='D')
    df_cur['date'] = pd.to_datetime(df_cur['date'], origin='1899-12-31', unit='D')

    df['description'] = df['description'].astype(str).str.strip()
    df['type']        = df['type'].astype(str).str.strip().str.upper()
    df['currency']    = df['currency'].astype(str).str.strip().str.upper()
    df['item']        = df['item'].astype(str).str.strip().str.upper()
    df['subitem']     = df['subitem'].astype(str).str.strip().str.upper()
    df['comment']     = df['comment'].astype(str).str.strip().str.upper()

    df['account']    = df['currency'].replace(TO_ACCOUNTS)
    df['currency']   = df['account'].replace(ACCOUNTS_TO_CURRENCIES)

    df = df.query('t_balance != 0')

    curr_change = None
    if base_currency=='USD':
        df, curr_change = convert_to_uzs(df,df_cur)
        df = convert_to_usd(df,df_cur)
    elif base_currency=='UZS':
        df, curr_change = convert_to_uzs(df,df_cur)

    df['item']        = df['item'].replace({'0':'Статья не назначена'})
    df['subitem']     = df['item'].replace({'0':'Статья не назначена'})
    df['t_balance_abs']     = abs(df['t_balance'])
    
    return df,curr_change

@st.cache_data      
def filter_df(df, start_date, end_date,currencies,accounts,items,subitems):
    df = df.copy()
    df = df.query(f'date >= "{start_date}"')\
            .query(f'date <= "{end_date}"')
    if currencies:
        df = df.query(f'currency in @currencies')
    if accounts:
        df = df.query(f'account in @accounts')
    if items:
        df = df.query(f'item in @items')
    if subitems:
        df = df.query(f'subitem in @subitems')
            
    return df

@st.cache_data    
def plot_balance_bar(df,date_granultaion,metric='t_balance',title='Деньги на счетах',low_balance = 10_000_000):
    
    df_temp = df.groupby([pd.Grouper(key='date',freq=date_granultaion)]).sum().reset_index().sort_values('date')
    df_temp['is_low'] = df_temp[metric] < low_balance

    line_color = ['red' if is_low else 'blue' for is_low in df_temp['is_low']]

    fig = px.bar(df_temp,'date',metric,color=line_color, title=title)

    fig.update_layout(showlegend=False, yaxis_title='', xaxis_title='')
    fig.update_layout(margin = dict(t=50, l=5, r=5, b=5))
    return fig

@st.cache_data
def plot_balance_line(df,date_granultaion,metric='t_balance',title='Деньги на счетах'):
    
    df_temp = df.groupby([pd.Grouper(key='date',freq=date_granultaion)]).sum().reset_index().sort_values('date')
    color =  'red' if df_temp['t_balance'].sum() < 0 else 'blue'

    fig = px.line(df_temp,'date',metric,title=title, markers=True, line_shape='spline',render_mode='svg')
    fig.update_traces(line_color=color)
    fig.update_layout(showlegend=False, yaxis_title='', xaxis_title='')
    return fig

@st.cache_data    
def plot_balance_parts(df,date_granultaion):
    income = plot_balance_line(df.query('type == "ПРИХОД"'),date_granultaion,metric='t_pos',title='Выручка') 
    outcome= plot_balance_line(df.query('type == "РАСХОД"'),date_granultaion,metric='t_neg',title='Расходы')   
    fig = income.add_trace(outcome.data[0]) 
    fig.update_layout(title='Выручка и Расходы')
    fig.update_layout(margin = dict(t=50, l=5, r=5, b=5))
    return fig

def sunburst(df,path=['item','subitem'], metric='t_sum',title=''):
    df = df.copy()

    fig = px.sunburst(df, path=path, values=metric,title=title)
    fig.update_layout(margin = dict(t=50, l=5, r=5, b=5))

    return fig

@st.cache_data    
def plot_debt_line(df_debt, date_granultaion):
    df_debt = df_debt.copy()
    df_temp = df_debt.groupby([pd.Grouper(key='date',freq=date_granultaion)]).sum()['t_balance'].cumsum().reset_index()
    fig = px.line(df_temp.reset_index(),
                  'date',
                  't_balance',
                  markers=True,
                  line_shape='spline',
                  render_mode='svg',
                  title='Суммарный баланс обязательств по выбранным контагентам')
    fig.update_layout(yaxis_title='', xaxis_title='')
    return fig

@st.cache_data    
def plot_debt_item_lines(df_debt, date_granultaion,debt_items):
    df_debt = df_debt.copy()
    if debt_items:
        df_debt = df_debt.query('item in @debt_items')
    df_temp = df_debt.groupby([pd.Grouper(key='date',freq=date_granultaion),'item']).sum()['t_balance'].groupby(level=1).cumsum().reset_index()
    fig = px.line(df_temp.reset_index(),
                  'date',
                  't_balance',
                  markers=True,
                  color='item',
                  line_shape='spline',
                  render_mode='svg',
                  title='Баланс задолжностей по выбранным контрагентам')
    fig.update_layout(yaxis_title='', xaxis_title='')
    return fig

def make_grid(cols,rows):
    grid = [0]*cols
    for i in range(cols):
        with st.container():
            grid[i] = st.columns(rows)
    return grid

def get_data(data_dir):
    filelist=[]
    for root, dirs, files in os.walk(data_dir):
          for file in files:
                 filename=os.path.join(root, file)
                 filelist.append(filename)
    return filelist

def save_file(content,file_path):
    with open(file_path, 'wb') as file:
        file.write(content)

def delete_file(file_path=''):
    if os.path.exists(file_path):
        os.remove(file_path)

# UI
st.sidebar.header("Модуль Управленческой Отчетности")

if datetime.datetime.now().month==12:
    st.snow()

convert_everything    = st.sidebar.checkbox('Конвертировать все операции в единую валюту')
base_currency = ''
if convert_everything:
    base_currency = st.sidebar.selectbox('Единая валюта',options=['UZS','USD'])

selected_granulation  = DATE_GRAN_DICT[st.sidebar.select_slider("Выбрать грануляцию",DATE_GRAN_DICT.keys())]


# with st.expander('данные'):

files = st.file_uploader("Загрузите отчетность", type=["xlsx", "xls", "xlsb"],accept_multiple_files =True)

for file in files:
    save_file(file.getvalue(),DATA_DIR+file.name) 
filelist = get_data(DATA_DIR)

with st.expander('📋: Менджер ввденных данных'):
    grid = make_grid(len(filelist),2)
    for i, file_name in enumerate(filelist):
        grid[i][0].write(file_name.replace(DATA_DIR,''))
        grid[i][1].button('Удалить', key='X'+file_name, use_container_width=True, on_click = delete_file, kwargs=dict(file_path=file_name))


if filelist:
    df = pd.concat([pd.read_excel(f) for f in filelist])
    df_original = df.copy().rename(columns = COLUMNS).drop(columns=COLUMNS_TO_DROP)
    df,curr_change = preprocess_df(df, df_cur,base_currency)

    st.header('🔍Фильтр по операциям')
    _, mid, _  = st.columns([0.2,4,0.2])
    with mid:
        start_date, end_date  =  date_range_picker("Выберете период",
                                                    default_start = datetime.date(2000, 1, 1),
                                                    default_end   = datetime.date.today())

    _, col1, col2, col3, col4, _  = st.columns([0.2,1,1,1,1,0.2])

    with col1:
        currencies = st.multiselect('Валюта', options=df_accs['currency'].unique())
    with col2:
        accounts   = st.multiselect('Счет', options=df_accs['account_id'])
    with col3:
        items      = st.multiselect('Категория', options=df['item'].unique())
    with col4:
        subitems   = st.multiselect('ПодКатегория', options=df['subitem'].unique())
    df = filter_df(df, start_date, end_date,currencies,accounts,items,subitems)


    with st.expander('🔍 Отфильтерованные данные'):
        df_show = df[COLUMNS_TO_SHOW].rename(columns=COLUMNS_TO_SHOW)
        st.write(df_show)



    st.header('💸 Операционная деятельность')
    income  = df.query('type == "ПРИХОД"')['t_balance_abs'].sum()
    outcome = df.query('type == "РАСХОД"')['t_balance_abs'].sum()
    balance = df['t_balance'].sum()    

    col1, col2, col3  = st.columns(3)
    col1.metric("Приход", f"{income:_}".replace('_',' '))
    col2.metric("Расход", f"{outcome:_}".replace('_',' '))
    col3.metric("Баланс", f"{balance:_}".replace('_',' '))
    with st.expander('💸 Операционная деятельность'):
        # col1, col2, col3  = st.columns(3)
        # income  = df.query('type == "ПРИХОД"')['t_balance_abs'].sum()
        # outcome = df.query('type == "РАСХОД"')['t_balance_abs'].sum()
        # balance = df['t_balance'].sum()

        # col1.metric("Приход", f"{income:_}".replace('_',' '))
        # col2.metric("Расход", f"{outcome:_}".replace('_',' '))
        # col3.metric("Баланс", f"{balance:_}".replace('_',' '))
        # style_metric_cards(border_left_color='#FF4B4B')
       
        
        balance_bar   = plot_balance_bar(df,selected_granulation,metric='t_balance',title='Деньги на счетах',low_balance = 10_000_000)
        st.plotly_chart(balance_bar,use_container_width=True)

        balance_parts = plot_balance_parts(df,selected_granulation)
        st.plotly_chart(balance_parts, use_container_width=True)

        col1, col2 = st.columns([1,1])
        with col1:
            sunburst_fig  = sunburst(df.query('type == "ПРИХОД"'),path=['item','subitem'], metric='t_balance_abs', title='Структра Прихода')
            st.plotly_chart(sunburst_fig, use_container_width=True)
        with col2:
            sunburst_fig  = sunburst(df.query('type == "РАСХОД"'),path=['item','subitem'], metric='t_balance_abs', title='Структра Расхода')
            st.plotly_chart(sunburst_fig, use_container_width=True)
        # with col3:
        #     sunburst_fig  = sunburst(df.query('type == "КАРЗ"'),path=['item','subitem'], metric='t_balance_abs', title='Структра Карз')
        #     st.plotly_chart(sunburst_fig, use_container_width=True)


# ------------------------------------------------

    df_debt              = df.query('`type` == "КАРЗ"').sort_values('date')
    df_debt_stats        = df_debt.groupby(['item']).sum().sort_values('t_balance')
    df_debt_stats['debt_type'] = (df_debt_stats['t_balance'] < 0).replace({True:'credit',False:'debit'})

    
    debt_debit    = df_debt_stats.query('t_balance >0')['t_balance'].sum()
    debt_closed   = df_debt_stats.query('t_balance ==0')['t_pos'].sum()
    debt_credit   = df_debt_stats.query('t_balance <0')['t_balance'].sum()

    st.header('💱 Обязательства')

    _, mid, _  = st.columns([0.2,4,0.2])
    with mid:
        debt_items = st.multiselect('Контрагент', options=df_debt_stats.index.unique())
    
    col1, col2, col3  = st.columns(3)
    col1.metric("Дебиторская задолжность", f"{debt_debit:_}".replace('_',' '))
    col2.metric("Кредиторская задолжность", f"{debt_credit:_}".replace('_',' '))
    col3.metric("Объем закрытых обязательств", f"{debt_closed:_}".replace('_',' '))

    with st.expander('💱 Обязательства'):  

        # df_debt              = df.query('`type` == "КАРЗ"').sort_values('date')
        # df_debt_stats        = df_debt.groupby(['item']).sum().sort_values('t_balance')
        # df_debt_stats['debt_type'] = (df_debt_stats['t_balance'] < 0).replace({True:'credit',False:'debit'})

        df_debt_selected     =  df_debt.copy()
        if debt_items:
            df_debt_stats    = df_debt_stats.loc[debt_items,:]
            df_debt_selected = df_debt.query('item in @debt_items')


        # col1, col2, col3  = st.columns(3)
        # debt_debit    = df_debt_stats.query('t_balance >0')['t_balance'].sum()
        # debt_closed   = df_debt_stats.query('t_balance ==0')['t_pos'].sum()
        # debt_credit   = df_debt_stats.query('t_balance <0')['t_balance'].sum()

        # col1.metric("Дебиторская задолжность", f"{debt_debit:_}".replace('_',' '))
        # col2.metric("Кредиторская задолжность", f"{debt_credit:_}".replace('_',' '))
        # col3.metric("Объем закрытых обязательств", f"{debt_closed:_}".replace('_',' '))
        # style_metric_cards(border_left_color='#FF4B4B')


        
        debt_line = plot_debt_line(df_debt_selected, selected_granulation)
        st.plotly_chart(debt_line, use_container_width=True)

        debt_lines = plot_debt_item_lines(df_debt, selected_granulation,debt_items)
        st.plotly_chart(debt_lines, use_container_width=True)


        col1, col2  = st.columns([4,2])
        with col1:
            st.write('Статистика по контагентам')
            st.write(df_debt_stats)

            st.write('Обязательства по валютам')
            df_debt_stats_bycurr = pd.pivot_table(df_debt_selected,index='currency',columns='item',values='t_balance', aggfunc='sum').fillna('')
            st.write(df_debt_stats_bycurr)
        with col2:
            sunburst_fig  = sunburst(df_debt_stats.query('t_balance !=0').reset_index(),path=['debt_type','item'], metric='t_balance_abs', title='Структра незакрытых обязательств')
            st.plotly_chart(sunburst_fig, use_container_width=True)

    
        st.write(df_debt_selected)


    st.header('📜 Справочные данные')

    with st.expander('📜 Справочные данные'):
        curr_book = df_original['currency'].astype(str).str.strip().replace(TO_ACCOUNTS).value_counts()
        curr_book = curr_book.reset_index().rename(columns={'currency':'Количество операций','index':'account_id'})
        curr_book = df_accs.merge(curr_book,on='account_id',how='right').set_index('account_id')
        curr_book = curr_book.rename(columns={'account_id':'Счет'})

        type_book = df_original['type'].astype(str).str.strip().value_counts()

        df_cur['date'] = pd.to_datetime(df_cur['date'], origin='1899-12-31', unit='D')
        df_cur['Курс'] = df_cur['to']+'-'+ df_cur['from']

        _,col1, col2, _ = st.columns([1,5,3,1])
        _,mid, _ = st.columns([1,8,1])
        with col1:
            st.write('Соответствие справочных счетов')
            if curr_book.isna().any().any():
                st.warning('В ввденных данных есть неизвестные *счета*', icon="⚠️")
            else:
                st.success('С *счетами* всё в порядке!', icon="✅")
            st.write(curr_book)
        with col2:
            st.write('Соответствие типов операций')
            if len(set({'РАСХОД','ПРИХОД','КАРЗ'}).intersection(set(type_book.index)))!=3:
                st.warning('В ввденных данных есть неизвестные *типы операций*', icon="⚠️")
            else:
                st.success('С *типами операций* всё в порядке!', icon="✅")
            st.write(type_book)
        with mid:
            st.info('Все в порядке с изменение курса?', icon="ℹ️")
            st.write('Курс согласно справочникам')
            st.plotly_chart(px.line(df_cur,'date','rate',color='Курс', markers=True, line_shape='spline',render_mode='svg'), use_container_width=True)
            if curr_change is not None:
                st.write('Курс по операциям')
                st.plotly_chart(px.line(curr_change,'date','rate',color='currency', markers=True, line_shape='spline',render_mode='svg'), use_container_width=True)


style_metric_cards(border_left_color='#ff4b4b')