from __future__ import annotations

# utils/mapping.py
lang_mapping = {
    "az-AZ": "aze_AZE",
    "bg-BG": "bul_BGR",
    "cs-CZ": "ces_CZE",
    "da-DK": "dan_DNK",
    "de-AT": "deu_DEU",
    "de-CH": "deu_CHE",
    "de-DE": "deu_DEU",
    "el-GR": "ell_GRC",
    "en": "eng_GLO",
    "en-AE": "eng_GLO",
    "en-CA": "eng_CAN",
    "en-GB": "eng_GBR",
    "en-HK": "eng_GLO",
    "en-IE": "eng_GLO",
    "en-IN": "eng_IND",
    "en-KE": "eng_GLO",
    "en-MY": "eng_MYS",
    "en-QA": "eng_GLO",
    "en-RS": "eng_GLO",
    "en-SA": "eng_GLO",
    "en-SG": "eng_GLO",
    "en-US": "eng_USA",
    "en-ZA": "eng_GLO",
    "es-ES": "spa_ESP",
    "es-MX": "spa_MEX",
    "es-PE": "spa_PER",
    "et-EE": "est_EST",
    "fi-FI": "fin_FIN",
    "fr-BE": "fra_FRA",
    "fr-CA": "fra_CAN",
    "fr-CH": "fra_CHE",
    "fr-FR": "fra_FRA",
    "fr-MA": "fra_FRA",
    "hr-HR": "eng_GLO",
    "hu-HU": "hun_HUN",
    "it-CH": "ita_CHE",
    "it-IT": "ita_ITA",
    "ka-GE": "kat_GEO",
    "lt-LT": "lit_LTU",
    "lv-LV": "lav_LVA",
    "nl-BE": "nld_NLD",
    "nl-NL": "nld_NLD",
    "no-NO": "nor_NOR",
    "pl-PL": "pol_POL",
    "pt-PT": "por_PRT",
    "ro-RO": "ron_ROU",
    "ru-BY": "rus_RUS",
    "ru-KZ": "rus_RUS",
    "ru-RU": "rus_RUS",
    "sk-SK": "slk_SVK",
    "sl-SI": "slv_SVN",
    "sv-SE": "swe_SWE",
    "tr-TR": "tur_TUR",
    "uk-UA": "ukr_UKR",
}
brand_mapping = {
    "frico": "FRI",
    "systemair": "SYS",
    "fantech": "FAN",
    "menerga": "MEN",
    "tekadoor": "TK"
}
market_mapping = [
  {
    "Name": "Systemair Global (005) - EN",
    "Market ID": "MARKET-005",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Frico Global (040) - EN/ZW",
    "Market ID": "MARKET-040",
    "Brand": "frico",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Frico Sweden (041) - SV/SE",
    "Market ID": "MARKET-041",
    "Brand": "frico",
    "shortcut PIM": "sv",
    "language PIM": "swe_SWE"
  },
  {
    "Name": "Systemair SE (115) - SV/SE",
    "Market ID": "MARKET-115",
    "Brand": "systemair",
    "shortcut PIM": "sv",
    "language PIM": "swe_SWE"
  },
  {
    "Name": "Systemair TW (225) - EN/TW",
    "Market ID": "MARKET-225",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair IN (240) - EN/IN",
    "Market ID": "MARKET-240",
    "Brand": "systemair",
    "shortcut PIM": "eni",
    "language PIM": "eng_IND"
  },
  {
    "Name": "Systemair AE (250) - EN/AE",
    "Market ID": "MARKET-250",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair GR (260) - EL/GR",
    "Market ID": "MARKET-260",
    "Brand": "systemair",
    "shortcut PIM": "gr",
    "language PIM": "ell_GRC"
  },
  {
    "Name": "Frico Netherland (280) - NL/NL",
    "Market ID": "MARKET-280",
    "Brand": "frico",
    "shortcut PIM": "nl",
    "language PIM": "nld_NLD"
  },
  {
    "Name": "Systemair PE (285) - ES/PE",
    "Market ID": "MARKET-285",
    "Brand": "systemair",
    "shortcut PIM": "spe",
    "language PIM": "spa_PER"
  },
  {
    "Name": "Systemair ZA (290) - EN/ZA",
    "Market ID": "MARKET-290",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair UK (300) - EN/GB",
    "Market ID": "MARKET-300",
    "Brand": "systemair",
    "shortcut PIM": "eng",
    "language PIM": "eng_GBR"
  },
  {
    "Name": "Systemair LT (310) - LT/LT",
    "Market ID": "MARKET-310",
    "Brand": "systemair",
    "shortcut PIM": "lt",
    "language PIM": "lit_LTU"
  },
  {
    "Name": "Systemair IT (325) - IT/IT",
    "Market ID": "MARKET-325",
    "Brand": "systemair",
    "shortcut PIM": "it",
    "language PIM": "ita_ITA"
  },
  {
    "Name": "Frico Italy (326) - IT/IT",
    "Market ID": "MARKET-326",
    "Brand": "frico",
    "shortcut PIM": "it",
    "language PIM": "ita_ITA"
  },
  {
    "Name": "Systemair MY (330) - EN/MY",
    "Market ID": "MARKET-330",
    "Brand": "systemair",
    "shortcut PIM": "enm",
    "language PIM": "eng_MYS"
  },
  {
    "Name": "Systemair RS (350) - EN/RS",
    "Market ID": "MARKET-350",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair HR (360) - HR/HR",
    "Market ID": "MARKET-360",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Menerga Global (380) - DE/DE",
    "Market ID": "MARKET-380",
    "Brand": "menerga",
    "shortcut PIM": "de",
    "language PIM": "deu_DEU"
  },
  {
    "Name": "Menerga Global (380) - EN/DE",
    "Market ID": "MARKET-380",
    "Brand": "menerga",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Frico France (410) - FR/FR",
    "Market ID": "MARKET-410",
    "Brand": "frico",
    "shortcut PIM": "fr",
    "language PIM": "fra_FRA"
  },
  {
    "Name": "Frico Norway (420) - NO/NO",
    "Market ID": "MARKET-420",
    "Brand": "frico",
    "shortcut PIM": "no",
    "language PIM": "nor_NOR"
  },
  {
    "Name": "Frico Germany (435) - DE/DE",
    "Market ID": "MARKET-435",
    "Brand": "frico",
    "shortcut PIM": "de",
    "language PIM": "deu_DEU"
  },
  {
    "Name": "Systemair FI (440) - FI/FI",
    "Market ID": "MARKET-440",
    "Brand": "systemair",
    "shortcut PIM": "fi",
    "language PIM": "fin_FIN"
  },
  {
    "Name": "Frico DK (445) - DA/DK",
    "Market ID": "MARKET-445",
    "Brand": "frico",
    "shortcut PIM": "da",
    "language PIM": "dan_DNK"
  },
  {
    "Name": "Systemair NO (450) - NO/NO",
    "Market ID": "MARKET-450",
    "Brand": "systemair",
    "shortcut PIM": "no",
    "language PIM": "nor_NOR"
  },
  {
    "Name": "Fantech Canada (460) - EN/CA",
    "Market ID": "MARKET-460",
    "Brand": "fantech",
    "shortcut PIM": "enc",
    "language PIM": "eng_CAN"
  },
  {
    "Name": "Fantech Canada (460) - FR/CA",
    "Market ID": "MARKET-460",
    "Brand": "fantech",
    "shortcut PIM": "frc",
    "language PIM": "fra_CAN"
  },
  {
    "Name": "Systemair CA (461) - EN/CA",
    "Market ID": "MARKET-461",
    "Brand": "systemair",
    "shortcut PIM": "enc",
    "language PIM": "eng_CAN"
  },
  {
    "Name": "Systemair CA (461) - FR/CA",
    "Market ID": "MARKET-461",
    "Brand": "systemair",
    "shortcut PIM": "frc",
    "language PIM": "fra_CAN"
  },
  {
    "Name": "Frico Canada (462) - EN/CA",
    "Market ID": "MARKET-462",
    "Brand": "frico",
    "shortcut PIM": "enc",
    "language PIM": "eng_CAN"
  },
  {
    "Name": "Frico Canada (462) - FR/CA",
    "Market ID": "MARKET-462",
    "Brand": "frico",
    "shortcut PIM": "frc",
    "language PIM": "fra_CAN"
  },
  {
    "Name": "Systemair PL (470) - PL/PL",
    "Market ID": "MARKET-470",
    "Brand": "systemair",
    "shortcut PIM": "pl",
    "language PIM": "pol_POL"
  },
  {
    "Name": "Frico Poland (471) - PL/PL",
    "Market ID": "MARKET-471",
    "Brand": "frico",
    "shortcut PIM": "pl",
    "language PIM": "pol_POL"
  },
  {
    "Name": "Systemair FR (480) - FR/FR",
    "Market ID": "MARKET-480",
    "Brand": "systemair",
    "shortcut PIM": "fr",
    "language PIM": "fra_FRA"
  },
  {
    "Name": "Systemair CZ (490) - CS/CZ",
    "Market ID": "MARKET-490",
    "Brand": "systemair",
    "shortcut PIM": "cs",
    "language PIM": "ces_CZE"
  },
  {
    "Name": "Frico Czech Republic (491) - CS/CZ",
    "Market ID": "MARKET-491",
    "Brand": "frico",
    "shortcut PIM": "cs",
    "language PIM": "ces_CZE"
  },
  {
    "Name": "Systemair SK (500) - SK/SK",
    "Market ID": "MARKET-500",
    "Brand": "systemair",
    "shortcut PIM": "sk",
    "language PIM": "slk_SVK"
  },
  {
    "Name": "Frico Slovakia (501) - SK/SK",
    "Market ID": "MARKET-501",
    "Brand": "frico",
    "shortcut PIM": "sk",
    "language PIM": "slk_SVK"
  },
  {
    "Name": "Systemair MX (535) - ES/MX",
    "Market ID": "MARKET-535",
    "Brand": "systemair",
    "shortcut PIM": "smx",
    "language PIM": "spa_MEX"
  },
  {
    "Name": "Systemair US (540) - EN/US",
    "Market ID": "MARKET-540",
    "Brand": "systemair",
    "shortcut PIM": "enu",
    "language PIM": "eng_USA"
  },
  {
    "Name": "Fantech US (541) - EN/US",
    "Market ID": "MARKET-541",
    "Brand": "fantech",
    "shortcut PIM": "enu",
    "language PIM": "eng_USA"
  },
  {
    "Name": "Frico USA (542) - EN/US",
    "Market ID": "MARKET-542",
    "Brand": "frico",
    "shortcut PIM": "enu",
    "language PIM": "eng_USA"
  },
  {
    "Name": "Systemair SI (610) - SL/SI",
    "Market ID": "MARKET-610",
    "Brand": "systemair",
    "shortcut PIM": "sl",
    "language PIM": "slv_SVN"
  },
  {
    "Name": "Systemair BG (625) - BG/BG",
    "Market ID": "MARKET-625",
    "Brand": "systemair",
    "shortcut PIM": "bg",
    "language PIM": "bul_BGR"
  },
  {
    "Name": "Systemair PT (635) - PT/PT",
    "Market ID": "MARKET-635",
    "Brand": "systemair",
    "shortcut PIM": "pt",
    "language PIM": "por_PRT"
  },
  {
    "Name": "Systemair ES (645) - ES/ES",
    "Market ID": "MARKET-645",
    "Brand": "systemair",
    "shortcut PIM": "sp",
    "language PIM": "spa_ESP"
  },
  {
    "Name": "Frico Spain (646) - ES/ES",
    "Market ID": "MARKET-646",
    "Brand": "frico",
    "shortcut PIM": "sp",
    "language PIM": "spa_ESP"
  },
  {
    "Name": "Systemair IE (655) - EN/IE",
    "Market ID": "MARKET-655",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair SG (660) - EN/SG",
    "Market ID": "MARKET-660",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair HK (665) - EN/HK",
    "Market ID": "MARKET-665",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair DE (670) - DE/DE",
    "Market ID": "MARKET-670",
    "Brand": "systemair",
    "shortcut PIM": "de",
    "language PIM": "deu_DEU"
  },
  {
    "Name": "Systemair AT (680) - DE/AT",
    "Market ID": "MARKET-680",
    "Brand": "systemair",
    "shortcut PIM": "de",
    "language PIM": "deu_DEU"
  },
  {
    "Name": "Systemair CH (685) - DE/CH",
    "Market ID": "MARKET-685",
    "Brand": "systemair",
    "shortcut PIM": "des",
    "language PIM": "deu_CHE"
  },
  {
    "Name": "Systemair CH (685) - FR/CH",
    "Market ID": "MARKET-685",
    "Brand": "systemair",
    "shortcut PIM": "frs",
    "language PIM": "fra_CHE"
  },
  {
    "Name": "Systemair CH (685) - IT/CH",
    "Market ID": "MARKET-685",
    "Brand": "systemair",
    "shortcut PIM": "its",
    "language PIM": "ita_CHE"
  },
  {
    "Name": "Systemair NL (695) - NL/NL",
    "Market ID": "MARKET-695",
    "Brand": "systemair",
    "shortcut PIM": "nl",
    "language PIM": "nld_NLD"
  },
  {
    "Name": "Systemair BE (700) - FR/BE",
    "Market ID": "MARKET-700",
    "Brand": "systemair",
    "shortcut PIM": "fr",
    "language PIM": "fra_FRA"
  },
  {
    "Name": "Systemair BE (700) - NL/BE",
    "Market ID": "MARKET-700",
    "Brand": "systemair",
    "shortcut PIM": "nl",
    "language PIM": "nld_NLD"
  },
  {
    "Name": "Systemair EE (730) - ET/EE",
    "Market ID": "MARKET-730",
    "Brand": "systemair",
    "shortcut PIM": "et",
    "language PIM": "est_EST"
  },
  {
    "Name": "Systemair LV (740) - LV/LV",
    "Market ID": "MARKET-740",
    "Brand": "systemair",
    "shortcut PIM": "lv",
    "language PIM": "lav_LVA"
  },
  {
    "Name": "Systemair BY (746) - RU/BY",
    "Market ID": "MARKET-746",
    "Brand": "systemair",
    "shortcut PIM": "ru",
    "language PIM": "rus_RUS"
  },
  {
    "Name": "Systemair TR (760) - TR/TR",
    "Market ID": "MARKET-760",
    "Brand": "systemair",
    "shortcut PIM": "tr",
    "language PIM": "tur_TUR"
  },
  {
    "Name": "Systemair DK (770) - DA/DK",
    "Market ID": "MARKET-770",
    "Brand": "systemair",
    "shortcut PIM": "da",
    "language PIM": "dan_DNK"
  },
  {
    "Name": "Systemair HU (790) - HU/HU",
    "Market ID": "MARKET-790",
    "Brand": "systemair",
    "shortcut PIM": "hu",
    "language PIM": "hun_HUN"
  },
  {
    "Name": "Systemair RO (795) - RO/RO",
    "Market ID": "MARKET-795",
    "Brand": "systemair",
    "shortcut PIM": "ro",
    "language PIM": "ron_ROU"
  },
  {
    "Name": "Systemair KZ (905) - RU/KZ",
    "Market ID": "MARKET-905",
    "Brand": "systemair",
    "shortcut PIM": "ru",
    "language PIM": "rus_RUS"
  },
  {
    "Name": "Systemair RU (920) - RU/RU",
    "Market ID": "MARKET-920",
    "Brand": "systemair",
    "shortcut PIM": "ru",
    "language PIM": "rus_RUS"
  },
  {
    "Name": "Frico Russia (921) - RU/RU",
    "Market ID": "MARKET-921",
    "Brand": "frico",
    "shortcut PIM": "ru",
    "language PIM": "rus_RUS"
  },
  {
    "Name": "Systemair GE (925) - KA/GE",
    "Market ID": "MARKET-925",
    "Brand": "systemair",
    "shortcut PIM": "ka",
    "language PIM": "kat_GEO"
  },
  {
    "Name": "Systemair UA (930) - UK/UA",
    "Market ID": "MARKET-930",
    "Brand": "systemair",
    "shortcut PIM": "uk",
    "language PIM": "ukr_UKR"
  },
  {
    "Name": "Systemair AZ (935) - AZ/AZ",
    "Market ID": "MARKET-935",
    "Brand": "systemair",
    "shortcut PIM": "az",
    "language PIM": "aze_AZE"
  },
  {
    "Name": "Systemair QA (950) - EN/QA",
    "Market ID": "MARKET-950",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair SA (955) - EN/SA",
    "Market ID": "MARKET-955",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair MA (960) - FR/MA",
    "Market ID": "MARKET-960",
    "Brand": "systemair",
    "shortcut PIM": "fr",
    "language PIM": "fra_FRA"
  },
  {
    "Name": "Systemair EA (965) - EN/MW",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/RW",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/TZ",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/BI",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/RE",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/ER",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/MG",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/MZ",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/SO",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/ZW",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/DJ",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/KE",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/MU",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/SC",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/UG",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/KM",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  },
  {
    "Name": "Systemair EA (965) - EN/ET",
    "Market ID": "MARKET-965",
    "Brand": "systemair",
    "shortcut PIM": "en",
    "language PIM": "eng_GLO"
  }
]
numeric_display_mapping = {
    "eng_GLO": {
        "Language": "en",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "deu_DEU": {
        "Language": "de",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "fra_FRA": {
        "Language": "fr",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "bul_BGR": {
        "Language": "bg",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "ces_CZE": {
        "Language": "cs",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "dan_DNK": {
        "Language": "da",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "deu_CHE": {
        "Language": "des",
        "Thousand_Separator": "'",
        "Decimal_Symbol": "."
    },
    "ell_GRC": {
        "Language": "gr",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "eng_CAN": {
        "Language": "enc",
        "Thousand_Separator": "",
        "Decimal_Symbol": "."
    },
    "eng_GBR": {
        "Language": "eng",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "eng_IND": {
        "Language": "eni",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "spa_CHL": {
        "Language": "scl",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "spa_MEX": {
        "Language": "smx",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "est_EST": {
        "Language": "et",
        "Thousand_Separator": "",
        "Decimal_Symbol": "."
    },
    "fin_FIN": {
        "Language": "fi",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "fra_BEL": {
        "Language": "frb",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "fra_CAN": {
        "Language": "frc",
        "Thousand_Separator": "",
        "Decimal_Symbol": "."
    },
    "fra_CHE": {
        "Language": "frs",
        "Thousand_Separator": "'",
        "Decimal_Symbol": "."
    },
    "hrv_HRV": {
        "Language": "hr",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "hun_HUN": {
        "Language": "hu",
        "Thousand_Separator": "",
        "Decimal_Symbol": "."
    },
    "ita_CHE": {
        "Language": "its",
        "Thousand_Separator": "'",
        "Decimal_Symbol": "."
    },
    "ita_ITA": {
        "Language": "it",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "lit_LTU": {
        "Language": "lt",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "lav_LVA": {
        "Language": "lv",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "eng_MYS": {
        "Language": "enm",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "nld_NLD": {
        "Language": "nl",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "nor_NOR": {
        "Language": "no",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "pol_POL": {
        "Language": "pl",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "por_PRT": {
        "Language": "pt",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "rus_RUS": {
        "Language": "ru",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "slv_SVN": {
        "Language": "sl",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "srp_SRB": {
        "Language": "sr",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "swe_SWE": {
        "Language": "sv",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "tur_TUR": {
        "Language": "tr",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "slk_SVK": {
        "Language": "sk",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "spa_ESP": {
        "Language": "sp",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "eng_USA": {
        "Language": "enu",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "spa_PER": {
        "Language": "spe",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "zho_CHN": {
        "Language": "chn",
        "Thousand_Separator": ",",
        "Decimal_Symbol": "."
    },
    "ron_ROU": {
        "Language": "ro",
        "Thousand_Separator": "",
        "Decimal_Symbol": ","
    },
    "aze_AZE": {
        "Language": "az",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "kat_GEO": {
        "Language": "ka",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    },
    "ukr_UKR": {
        "Language": "uk",
        "Thousand_Separator": ".",
        "Decimal_Symbol": ","
    }
}



def map_locale(locale: str) -> str:
    """Map incoming locale (e.g. `en-GB`) to EPIM language code."""
    try:
        return lang_mapping[locale].lower()
    except KeyError as exc:
        raise ValueError(f"Unsupported locale: {locale}") from exc


def unmap_locale(locale: str) -> str | None:
    """Return the public locale for a given EPIM language code."""
    reverse = {value.lower(): key for key, value in lang_mapping.items()}
    return reverse.get(locale.lower())


def map_brand(brand: str) -> str:
    """Map a brand to its shorthand identifier."""
    try:
        return brand_mapping[brand.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported brand: {brand}") from exc


def map_market(brand: str, language_pim: str) -> str:
    for entry in market_mapping:
        if entry["Brand"].lower() == brand.lower() and entry["language PIM"].lower() == language_pim.lower():
            return entry["Market ID"]
    return brand


def get_epimLang_by_market(market: str) -> list[str]:
    market_ci = market.casefold()
    langs: list[str] = []
    seen: set[str] = set()
    for entry in market_mapping:
        if str(entry.get("Market ID", "")).casefold() == market_ci:
            lang = entry.get("language PIM")
            if lang and lang not in seen:
                seen.add(lang)
                langs.append(lang)
    return langs


def get_fallback_chain(lang: str) -> list[str]:
    if lang == "eng_glo":
        return [lang]
    fallback_map = {
        "deu_che": ["deu_che", "deu_deu", "eng_glo"],
        "ita_che": ["ita_che", "ita_ita", "eng_glo"],
        "fra_che": ["fra_che", "fra_fra", "eng_glo"],
        "ukr_ukr": ["ukr_ukr", "rus_rus", "eng_glo"],
    }
    return fallback_map.get(lang, [lang, "eng_glo"])
