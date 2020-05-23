import numpy as np
import pinyin
import pandas as pd
import re
from pandarallel import pandarallel
import logging

def get_keyword_drop_list():
    return ["科技", "商贸", "贸易", "设备", "投资", "器械", "义齿", "科贸",'医疗设备','有限公司',
            "经贸", "器材", "材料", "技术", "实业", "生物", "药业", "租赁", '市场']

def get_num_chairs(s):
    s = str(s)
    if re.search(r'治疗椅位[\d]+', s) is not None:
        m = re.search(r'治疗椅位[\d]+', s)
        num_chairs = np.int(m.group().replace('治疗椅位', ''))
    elif re.search(r'治疗台位[\d]+', s) is not None:
        m = re.search(r'治疗台位[\d]+', s)
        num_chairs = np.int(m.group().replace('治疗台位', ''))
    elif re.search(r'治疗椅[\d]+', s) is not None:
        m = re.search(r'治疗椅[\d]+', s)
        num_chairs = np.int(m.group().replace('治疗椅', ''))
    # 牙科综合治疗台200余台
    # 口腔综合治疗台72台
    elif re.search(r'治疗台[\d]+', s) is not None:
        m = re.search(r'治疗台[\d]+', s)
        num_chairs = np.int(m.group().replace('治疗台', ''))
    # 20台综合治疗台
    elif re.search(r'[\d]+台综合治疗台', s) is not None:
        m = re.search(r'[\d]+台综合治疗台', s)
        num_chairs = np.int(m.group().replace('台综合治疗台', ''))
    elif re.search(r'[\d]+张综合治疗椅', s) is not None:
        m = re.search(r'[\d]+张综合治疗椅', s)
        num_chairs = np.int(m.group().replace('张综合治疗椅', ''))
    elif re.search(r'[\d]+张综合治疗椅', s) is not None:
        m = re.search(r'[\d]+张综合治疗椅', s)
        num_chairs = np.int(m.group().replace('张综合治疗椅', ''))
    # 诊疗椅位110台
    elif re.search(r'诊疗椅位[\d]+', s) is not None:
        m = re.search(r'诊疗椅位[\d]+', s)
        num_chairs = np.int(m.group().replace('诊疗椅位', ''))
    else:
        num_chairs = np.nan
    return num_chairs


def guess_dealer(row, name_col):
    name = row[name_col]
    drop_list = get_keyword_drop_list()
    if any(word in name for word in drop_list):
        row['type'] = 'dealer'
    else:
        row['type'] = 'clinic'
    return row


def guess_dealer_for_basic_info(row):
    return guess_dealer(row, 'name')


def guess_dealer_for_sell_out(row):
    return guess_dealer(row, '客户名称')



def get_province_short(province):
    # province_common_words = ['市', '省', '自治区', '地区']
    return province \
        .replace('市', '') \
        .replace('省', '') \
        .replace('自治区', '') \
        .replace('地区', '')


def get_city_short(city):
    # city_common_words = ['市', '自治州', '地区', '县', '盟']
    return city\
        .replace('市', '') \
        .replace('县', '') \
        .replace('自治州', '') \
        .replace('盟', '') \
        .replace('地区', '')


def get_county_short(county):
    # city_common_words = ['旗', '市', '区', '县', '岛']
    return county if len(county) <= 2 else county\
        .replace('市', '') \
        .replace('自治县', '') \
        .replace('县', '') \
        .replace('旗', '') \
        .replace('岛', '') \
        .replace('区', '')


def strip_name(row, entities, name_col, entity_col):
    for entity in entities:
        #print(entity)
        #print(row[name_col])
        #print(str(entity) in str(row[name_col]))
        if str(entity) in str(row[name_col]) \
                and str(entity) + '路' not in str(row[name_col]) \
                and str(entity) + '中路' not in str(row[name_col]) \
                and str(entity) + '东路' not in str(row[name_col]) \
                and str(entity) + '南路' not in str(row[name_col]) \
                and str(entity) + '西路' not in str(row[name_col]) \
                and str(entity) + '北路' not in str(row[name_col]):
            row[name_col] = str(row[name_col]).replace(entity, '')
            row[name_col] = str(row[name_col]).replace('省', '')
            row[name_col] = str(row[name_col]).replace('旗', '')
            row[name_col] = str(row[name_col]).replace('岛', '')
            row[name_col] = str(row[name_col]).replace('市', '')
            row[name_col] = str(row[name_col]).replace('自治区', '')
            row[name_col] = str(row[name_col]).replace('地区', '')
            row[name_col] = str(row[name_col]).replace('辖区', '')
            row[name_col] = str(row[name_col]).replace('区', '')
            row[name_col] = str(row[name_col]).replace('县', '')
            #print(row[entity_col])
            #print(type(row[entity_col]))
            if row[entity_col] is np.nan or \
                    row[entity_col] == '':
                row[entity_col] = entity
    return row


ppc = pd.read_excel('/Users/xi_liang/workspace/studies/Dental_v2/dental/dental/data/01_raw/pcc_unique.xlsx')
provinces = ppc['province'].unique().tolist()
provinces_short = list(map(get_province_short, provinces))
cities = ppc['city'].unique().tolist()
cities_short = list(map(get_city_short, cities))
counties = ppc['county'].unique().tolist()
counties_short = list(map(get_county_short, counties))
province_short_long_map = dict(zip(provinces_short, provinces))
city_short_long_map = dict(zip(cities_short, cities))
county_short_long_map = dict(zip(counties_short, counties))

pcc_unique = pd.read_excel('/Users/xi_liang/workspace/studies/Dental_v2/dental/dental/data/01_raw/pcc_unique.xlsx')
county_city_map =dict(zip(pcc_unique['county'].to_list(), pcc_unique['city'].to_list()))
pc_unique = pd.read_excel('/Users/xi_liang/workspace/studies/Dental_v2/dental/dental/data/01_raw/pc_unique.xlsx')

city_province_map =dict(zip(pc_unique['city'].to_list(), pc_unique['province'].to_list()))


province_standard = pd.read_excel('/Users/xi_liang/workspace/studies/Dental_v2/dental/dental/data/01_raw/province_pinyin.xlsx')

def fill_target_with_source(row, target_col, source_col, target_source_map):
    row[target_col] = target_source_map.get(row[source_col]) \
        if target_source_map.get(row[source_col]) \
        else row[target_col]
    return row


def fill_province_use_city(row):
    return fill_target_with_source(row, 'province', 'city', city_province_map) \
        if row['province'] is np.nan or row['province'] == '' else row


def fill_city_use_county(row):
    return fill_target_with_source(row, 'city', 'county', county_city_map)\
        if row['city'] is np.nan or row['city'] == '' \
        else row


def make_short_long(row, short_long_map, map_col):
    row[map_col] = short_long_map[row[map_col]] if short_long_map.get(row[map_col]) else row[map_col]
    return row


def make_short_province_long(row):
    return make_short_long(row, province_short_long_map, 'province')


def make_short_city_long(row):
    return make_short_long(row, city_short_long_map, 'city')


def make_short_county_long(row):
    return make_short_long(row, county_short_long_map, 'county')


def strip_name_province(row):
    # print('... stripping province from name')
    return strip_name(
        strip_name(row, provinces, 'name', 'province'),
        provinces_short, 'name', 'province'
    )


def strip_name_city(row):
    # print('... stripping city from name')
    return strip_name(
        strip_name(row, cities, 'name', 'city'),
        cities_short, 'name', 'city'
    )



def strip_name_county(row):
    # print('... stripping county from name')
    return strip_name(
        strip_name(row, counties, 'name', 'county'),
        counties_short, 'name', 'county'
    )


def strip_address_province(row):
    # print('... stripping province from address')
    return strip_name(
        strip_name(row, provinces, 'address', 'province'),
        provinces_short, 'address', 'province'
    )


def strip_address_city(row):
    # print('... stripping city from name')
    return strip_name(
        strip_name(row, cities, 'address', 'city'),
        cities_short, 'address', 'city'
    )


def strip_province_city(row):
    # print('... stripping city from province')
    return strip_name(
        strip_name(row, cities, 'province', 'city'),
        cities_short, 'province', 'city'
    )


def strip_address_county(row):
    # print('... stripping city from address')
    return strip_name(
        strip_name(row, counties, 'address', 'county'),
        counties_short, 'address', 'county'
    )


def strip_words(name, words):
    for word in words:
        name = name.replace(word, '')
    return name


def strip_common_words(name):
    common_words = [
        '口腔医院', '医院', '第一门诊部', '第二门诊部', '第三门诊部', '门诊部',
        '口腔科', '门诊', '第三医学中心', '第四医学中心', '第五医学中心', '第六医学中心',
        '第七医学中心', '第八医学中心', '院区', '口腔', '口腔诊所', '牙科诊所', '诊所',
        '卫生院', '有限公司', '责任', '牙科', '齿科', '服务中心', '医疗美容',
        '牙病', '新院', '分院', '（', '）', '集团', '(', ')', '牙体', '牙髓', '口腔修复',
        '修复', '口腔颌面外', '牙周', '正畸', '粘膜', '预防', '儿童口腔', '综合', '放射',
        '急诊', '种植', '修复', '牙周', '综合', '技工', '技工'
    ]
    return strip_words(name, common_words)


def strip_common_words2(name):
    common_words = [
        '口腔医院', '医院', '第一门诊部', '第二门诊部', '第三门诊部', '门诊部',
        '口腔科', '门诊', '第三医学中心', '第四医学中心', '第五医学中心', '第六医学中心',
        '第七医学中心', '第八医学中心', '院区', '口腔', '口腔诊所', '牙科诊所', '诊所',
        '卫生院', '有限公司', '责任', '牙科', '齿科', '服务中心', '医疗美容',
        '牙病', '新院', '分院', '（', '）', '集团', '(', ')', '牙体', '牙髓', '口腔修复',
        '修复', '口腔颌面外', '牙周', '正畸', '粘膜', '预防', '儿童口腔', '综合', '放射',
        '急诊', '种植', '修复', '牙周', '综合', '技工', '技工', '医疗器械','医疗科技',
        '贸易', '医疗器械贸易',
    ]
    return strip_words(name, common_words)


def strip_common_words_from_name(row):
    row['strip_name'] = strip_common_words(row['name'])
    return row


def strip_common_words_from_name_additional_keywords(row):
    row['strip_name'] = strip_common_words2(row['name'])
    return row


def strip_external_words(name):
    return strip_words(name, ['口腔门诊部', '口腔门诊', '门诊部', '门诊'])


def strip_words_from_external_name(row):
    row['name'] = strip_external_words(row['name'])
    return row


def get_pinyin(str_):
    return pinyin.get(str_, format='strip')


def pinyin_to_chinese(pinyin_, entities):
    pinyin_chinese_map = dict(zip(list(map(get_pinyin, entities)), entities))
    # print(type(pinyin_))
    return pinyin_chinese_map.get(str(pinyin_).lower()) \
        if type(pinyin_) is str and pinyin_chinese_map.get(str(pinyin_).lower()) \
        else pinyin_


def pinyin_to_province(row):
    row['province'] = pinyin_to_chinese(row['province'], provinces_short)
    return row


def strip_pinyin_from_city(row):
    if type(row['city']) is str:
        row['city'] = re.sub("[a-zA-z]", "", row['city']).replace('(', '').replace(')', '')
    return row

def process(basic_info):
    log = logging.getLogger(__name__)
    log.info("-------------------------------")
    log.info("Processing name and addresses...")

    pandarallel.initialize()
    basic_info['original_address'] = basic_info.address
    #basic_info = basic_info[basic_info.province.isna() & basic_info.city.isna() & basic_info.county.isna()]
    #basic_info = basic_info.iloc[1:250,]
    #basic_info = basic_info[basic_info.original_name=='瑞橙斯口腔']
    basic_info['city'] = basic_info.city.fillna('')
    basic_info['province'] = basic_info.province.fillna('')
    basic_info['county'] = basic_info.county.fillna('')


    basic_info = basic_info.parallel_apply(strip_name_province, axis=1)
    # TODO: some city in name
    basic_info = basic_info.parallel_apply(strip_name_city, axis=1)


    # TODO: some county in name
    basic_info = basic_info.parallel_apply(strip_name_county, axis=1)


    # TODO: some province in address

    basic_info = basic_info.parallel_apply(strip_address_province, axis=1)
    # TODO: some city in address
    basic_info.parallel_apply(strip_address_city, axis=1)
    basic_info['city'] = basic_info['city'] \
        .parallel_apply(lambda x: str(x).replace('，', '').replace(',', '').replace('\'', ''))
    # TODO: some county in address
    basic_info = basic_info.parallel_apply(strip_address_county, axis=1)


    # TODO: some city in prov so change it
    basic_info = basic_info.parallel_apply(strip_province_city, axis=1)
    # TODO: some prov is pinyin so change it
    basic_info = basic_info.parallel_apply(pinyin_to_province, axis=1)
    # TODO: some city is pinyin so change it
    basic_info = basic_info.parallel_apply(strip_pinyin_from_city, axis=1)
    basic_info = basic_info.parallel_apply(make_short_province_long, axis=1)
    basic_info = basic_info.parallel_apply(make_short_city_long, axis=1)

    basic_info = basic_info.parallel_apply(make_short_county_long, axis=1)
    basic_info['province'].fillna('', inplace=True)
    basic_info['province'] = basic_info['province'].parallel_apply(lambda x: x.replace('nan', ''))
    basic_info['city'].fillna('', inplace=True)
    basic_info['city'] = basic_info['city'].parallel_apply(lambda x: x.replace('nan', ''))
    basic_info['county'].fillna('', inplace=True)
    basic_info['county'] = basic_info['county'].parallel_apply(lambda x: x.replace('nan', ''))
    basic_info = basic_info.parallel_apply(strip_common_words_from_name, axis=1)
    basic_info = basic_info.parallel_apply(fill_city_use_county, axis=1)


    basic_info = basic_info.parallel_apply(fill_province_use_city, axis=1)
    basic_info = basic_info.merge(province_standard, how='left',
                                              left_on='province', right_on='province')
    #print(basic_info.columns)

    basic_info.loc[~basic_info.province_standard.isna(), 'province'] = basic_info.loc[~basic_info.province_standard.isna(), 'province_standard']
    #basic_info.drop(columns=['province_standard'], inplace=True)


    basic_info = basic_info.reset_index()
    return basic_info


