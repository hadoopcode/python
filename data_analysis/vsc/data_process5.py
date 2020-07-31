from data_load import DataLoad


class DataProcess(object):
    def __init__(self, path):
        self.dl = DataLoad(path)

    def get_phrase(self, data):
        return self.dl.get_specified_lines(data, r'Title\D+:(.*)?')

    def get_phrase_list(self, data):
        phrase = self.get_phrase(data)
        phrase_list = phrase.split(';')
        phrase_list = [i.strip() for i in phrase_list]
        print(phrase_list)
        return phrase_list

    def get_IP_main(self, data):
        ip_list = self.dl.get_IP_list(data)
        ip_main = set()
        for ip in ip_list:
            ip_main.add(ip[:4])
        return list(ip_main)

    def get_TI_WI(self, data):
        ip_main = self.get_IP_main(data)
        phrase_list = self.get_phrase_list(data)
        ti_wi = list()
        for ip in ip_main:
            for phrase in phrase_list:
                ti_wi.append(phrase+'('+ip+')')
        return ti_wi

    def get_all_TI_WI(self):
        all_record = self.dl.get_all_records_base_tda()
        results = list()
        for record in all_record:
            results.extend(self.get_TI_WI(record))
        return results


if __name__ == "__main__":
    dp = DataProcess(r"F:\project\vsc_workplace\data")
    a = dp.get_all_TI_WI()
    print(a)
