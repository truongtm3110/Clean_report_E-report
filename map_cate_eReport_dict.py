import pandas as pd
import helper.helper_eReport

path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_cate_2907.xlsx"

df = pd.read_excel(path, sheet_name="Sheet1")
dict_ = df.to_dict(orient='records')

lst_dict_remake = []

for pro in dict_:
    name = pro["name"]
    cate = pro["category_name"]
    remove = pro["remove_edited"]

    if remove != "x":
        if cate in ["Mô tô, xe máy",  "Ô tô"]:
            cate = "Mô tô, xe máy"
        if cate in ['Camera giám sát']:
            cate = 'Cameras & Flycam'
        elif cate in ["Giày Dép Nữ"]:
            cate = "Giày Dép Nam"
        elif cate in ["Thời Trang Nam", "Thể Thao & Dã Ngoại","Phụ Kiện Thời Trang","Thời trang trẻ em & trẻ sơ sinh", "Mẹ & Bé"]:
            cate = "Thời Trang Nữ"
        elif cate in [ 'Túi Ví Nam']:
            cate =  'Túi Ví Nữ'
        elif cate in [ 'Chăm sóc tóc']:
            cate =  'Chăm sóc cá nhân'
        elif cate in ['Chuột & Bàn Phím']:
            cate =  'Máy tính & Laptop'
        elif cate in ['Sức Khỏe']:
            cate =  'Sắc Đẹp'
        elif cate in ['Thiết Bị Âm Thanh']:
            cate =  'Thiết Bị Điện Gia Dụng'
        elif cate in ['Đồ gia dụng nhà bếp', "Nhà cửa & Đời sống"]:
            cate = "Nhà cửa & Đời sống"

        lst_dict_remake.append({
            "name": name,
            "cate": cate
        })


lst_cate = set(d['cate'] for d in lst_dict_remake)


dict_cate_lv1 = {cate: [] for cate in lst_cate}
dict_cate_lv2 = {cate: [] for cate in lst_cate}
dict_cate_lv3 = {cate: [] for cate in lst_cate}

for pro in lst_dict_remake:
    name = pro['name']
    cate = pro['cate']

    cat1 = " ".join(name.split(" ")[:1])
    cat2 = " ".join(name.split(" ")[:2])
    cat3 = " ".join(name.split(" ")[:3])

    # Thêm vào các dict
    if cate in dict_cate_lv1:
        dict_cate_lv1[cate].append(cat1)
        dict_cate_lv2[cate].append(cat2)
        dict_cate_lv3[cate].append(cat3)


from collections import Counter
def get_dict(lst_input: list, num: int):
    lst_cate = Counter(lst_input)
    dict_cate_greater_than_num = {k: v for k, v in lst_cate.items() if v > num}
    return dict_cate_greater_than_num

lst_output = []

for cate in lst_cate:
    dict_lv1 = get_dict(dict_cate_lv1[cate], 50)
    dict_lv2 = get_dict(dict_cate_lv2[cate], 20)
    dict_lv3 = get_dict(dict_cate_lv3[cate], 5)

    for lv1_name, lv1_volume in dict_lv1.items():
        lst_children_lv2 = []

        for lv2_name, lv2_volume in dict_lv2.items():
            if lv2_name.startswith(f"{lv1_name} "):
                lst_children_lv3 = []
                lv2_total_volume = lv2_volume
                for lv3_name, lv3_volume in dict_lv3.items():
                    if lv3_name.startswith(f"{lv2_name} "):

                        if lv3_volume > 0.8 * lv2_volume:
                            print(lv2_name, lv2_volume,lv3_name, lv3_volume)
                            # Nếu volume của lv3 lớn hơn 80% volume của lv2, chuyển lv3 lên lv2
                            lst_children_lv2.append({
                                "name_report": lv3_name,
                                "volume_lv2": lv3_volume,
                                "level": 2
                            })
                        else:
                            lst_children_lv3.append({
                                "name_report": lv3_name,
                                "volume_lv3": lv3_volume,
                                "level": 3
                            })
                            # lv2_total_volume += lv3_volume
                            d_lv2 = {
                                "name_report": lv2_name,
                                "volume_lv2": lv2_total_volume,
                                "level": 2,
                                "children": lst_children_lv3
                            }
                            lst_children_lv2.append(d_lv2)

                # if lst_children_lv3:
                #     d_lv2 = {
                #         "name_report": lv2_name,
                #         "volume_lv2": lv2_total_volume,
                #         "level": 2,
                #         "children": lst_children_lv3
                #     }
                #     lst_children_lv2.append(d_lv2)

        if lst_children_lv2 or lv1_volume>0:
            d_lv1 = {
                "name_catel_lv1": cate,
                "name_cate_edited": lv1_name,
                "volume": lv1_volume,
                "level": 1,
                "children": lst_children_lv2
            }
            lst_output.append(d_lv1)


lst_output_sorted = sorted(lst_output, key=lambda x: (x['name_cate_edited']))

for lv1 in lst_output_sorted:
    if 'children' in lv1:
        lv1['children'] = sorted(lv1['children'], key=lambda x: (x['name_report']))

print("Ca te go rí:",lst_output_sorted)

result = {}

for item in lst_output_sorted:
    name_cate_edited = item['name_cate_edited']
    if name_cate_edited not in result:
        result[name_cate_edited] = {}

    for child_lv2 in item['children']:
        name_report_lv2 = child_lv2['name_report']
        if name_report_lv2 not in result[name_cate_edited]:
            result[name_cate_edited][name_report_lv2] = set()

        for child_lv3 in child_lv2.get('children', []):
            name_report_lv3 = child_lv3['name_report']
            result[name_cate_edited][name_report_lv2].add(name_report_lv3)

def check_number(s):
    for char in s:
        if char.isdigit():
            return True
    return False

def find_cate(name, dict_cate):
    name_spl = name.split()
    if check_number(name_spl[0]):
        name = " ".join(name_spl[1:])

    for lv1_name, lv2_dict in dict_cate.items():
        if isinstance(lv2_dict, dict):
            for lv2_name, lv3_set in lv2_dict.items():
                if isinstance(lv3_set, set):
                    for lv3_name in lv3_set:
                        if name.startswith(f"{lv3_name} ") or name == lv3_name:
                            return lv1_name, lv2_name, lv3_name

    for lv1_name, lv2_dict in dict_cate.items():
        if isinstance(lv2_dict, dict):
            for lv2_name in lv2_dict:
                if name.startswith(f"{lv2_name} ") or name == lv2_name:
                    return lv1_name, lv2_name, None

    for lv1_name in dict_cate:
        if name.startswith(f"{lv1_name} ") or name == lv1_name:
            return lv1_name, None, None

    return None, None, None

for idx, row in df.iterrows():
    lv1_name, lv2_name, lv3_name = find_cate(row['name'], result)
    df.at[idx, 'cat1_name'] = lv1_name
    df.at[idx, 'cat2_name'] = lv2_name
    df.at[idx, 'cat3_name'] = lv3_name

output_path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_2907_3.xlsx"
helper.helper_eReport.df_to_excel(df,output_path, index=False)
