from numpy.core.numeric import full
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def parse_source_json(source_json_raw, source_uid_col, source_profiletype_col,
                      source_pn_col, source_aka_col, source_lqaka_col, source_osn_col,
                      source_program_col, source_notes_col, source_dob_col,
                      source_id_col, source_nationality_col, source_residency_col,
                      source_address_col, source_pob_col, source_address_count_col,
                      source_pob_count_col, source_surname_col, source_aka_surname_col, as_json=False, **kwargs):
    parsed_source = []
    for profile_raw in source_json_raw:
        parsed_profile = {}
        source_uid = profile_raw["REFERENCE_NUMBER"]
        source_program = source_uid[:2]
        parsed_profile[source_uid_col] = source_uid
        parsed_profile[source_profiletype_col] = profile_raw[source_profiletype_col]
        parsed_profile[source_program_col] = source_program
        parsed_profile[source_pn_col], parsed_profile[source_surname_col] = get_primary_name(
            profile_raw, source_program, source_profiletype_col)
        aka_list, lqaka_list, parse_notes, parse_aka_dob, parse_surname_list = get_alias_details(
            profile_raw, source_profiletype_col, source_program)
        if len(aka_list) > 0:
            parsed_profile[source_aka_col] = aka_list
            parsed_profile[source_aka_surname_col] = parse_surname_list
        if len(lqaka_list) > 0:
            parsed_profile[source_lqaka_col] = lqaka_list
        if len(parse_notes) > 0:
            parsed_profile[source_notes_col] = parse_notes
        if len(parse_aka_dob) > 0:
            parsed_profile[source_dob_col] = parse_aka_dob
        parsed_profile[source_osn_col] = profile_raw.get(
            "NAME_ORIGINAL_SCRIPT")

        if parsed_profile.get(source_dob_col) is None:
            parsed_profile[source_dob_col] = get_dob(profile_raw)
        else:
            parsed_profile[source_dob_col] += get_dob(profile_raw)

        parsed_identity = get_identity(profile_raw, source_profiletype_col)
        if len(parsed_identity) > 0:
            parsed_profile[source_id_col] = parsed_identity

        parsed_nationality = get_nationality(profile_raw)
        if len(parsed_nationality) > 0:
            parsed_profile[source_nationality_col] = parsed_nationality

        parsed_residency = get_residency(profile_raw, source_profiletype_col)
        if len(parsed_residency) > 0:
            parsed_profile[source_residency_col] = parsed_residency

        parsed_address = get_address(profile_raw, source_profiletype_col)
        if len(parsed_address) > 0:
            parsed_profile[source_address_col] = parsed_address
            parsed_profile[source_address_count_col] = len(parsed_address)

        parsed_pob = get_pob(profile_raw)
        if len(parsed_pob) > 0:
            parsed_profile[source_pob_col] = parsed_pob
            parsed_profile[source_pob_count_col] = len(parsed_pob)

        parsed_source.append(parsed_profile)
    logger.info(
        f"Raw List has {len(source_json_raw)} entries. Parsed List has {len(parsed_source)} entries")
    if as_json:
        return parsed_source
    else:
        return pd.DataFrame(parsed_source).fillna("")


def get_pob(profile_raw):

    pob_list = profile_raw.get("INDIVIDUAL_PLACE_OF_BIRTH", None)

    if pob_list is not None:
        if type(pob_list) is not list:
            pob_list = [pob_list]

        out_list = []
        for pob in pob_list:
            full_pob = combine_address(pob)

            if full_pob != "":
                out_list.append(full_pob)

        return out_list

    else:
        return []


def combine_pob(pob_details):
    city = pob_details.get("CITY", "")
    state_province = pob_details.get("STATE_PROVINCE", "")
    country = pob_details.get("COUNTRY", "")

    out_pob = []
    for element in [city, state_province, country]:
        if (element != "") and (element is not None):
            out_pob.append(element)

    if len(out_pob) > 0:
        return ", ".join(out_pob)

    else:
        return ""


def combine_address(country_details):
    street = country_details.get("STREET", "")
    city = country_details.get("CITY", "")
    state_province = country_details.get("STATE_PROVINCE", "")
    country = country_details.get("COUNTRY", "")
    notes = country_details.get("NOTE", "")

    out_address = []
    for element in [street, city, state_province, country]:
        if (element != "") and (element is not None):
            out_address.append(element)

    if len(out_address) > 0:
        return ", ".join(out_address)

    else:
        return notes


def get_address(profile_raw, source_profiletype_col):
    profile_type = profile_raw[source_profiletype_col]
    if profile_type == "Entity":
        address_key = "ENTITY_ADDRESS"

    elif profile_type == "Individual":
        address_key = "INDIVIDUAL_ADDRESS"

    address_list = profile_raw.get(address_key, None)

    if address_list is not None:
        if type(address_list) is not list:
            address_list = [address_list]

        out_list = []
        for address in address_list:
            full_address = combine_address(address)

            if full_address != "":
                out_list.append(full_address)

        return out_list

    else:
        return []


def get_nationality(profile_raw):
    key = "NATIONALITY"
    out = []
    if key in profile_raw:
        nationality_raw = profile_raw[key]["VALUE"]
        if type(nationality_raw) is list:
            out = nationality_raw
        else:
            out = [nationality_raw]
    return out


def get_residency(profile_raw, source_profiletype_col):
    out = []
    profile_type = profile_raw[source_profiletype_col]
    if profile_type == "Entity":
        alias_key = "ENTITY_ADDRESS"

    elif profile_type == "Individual":
        alias_key = "INDIVIDUAL_ADDRESS"

    if alias_key in profile_raw:
        address_raw = profile_raw[alias_key]

        if type(address_raw) is dict:
            country = address_raw.get("COUNTRY", None)
            if country is not None:
                out.append(country)

        elif type(address_raw) is list:
            for address in address_raw:
                country = address.get("COUNTRY", None)
                if country is not None:
                    out.append(country)

        else:
            if address_raw is not None:
                logger.info(
                    f"UID: {profile_raw['REFERENCE_NUMBER']}. Address is not Dictionary or List")

    return out


def get_dob(profile_raw):
    dob_raw = profile_raw.get("INDIVIDUAL_DATE_OF_BIRTH", None)
    parsed_dob = []
    if dob_raw is not None:
        if type(dob_raw) is dict:
            dob_list = [dob_raw]

        elif type(dob_raw) is list:
            dob_list = dob_raw

        else:
            logger.error(f"dob_raw: {dob_raw}")
            raise TypeError("dob_list type is not dict or list.")

        for dob in dob_list:
            dob_date = dob.get("DATE", None)
            dob_year = dob.get("YEAR", None)
            dob_type = dob.get("TYPE_OF_DATE", None)
            if dob_date is not None:
                parsed_dob.append(dob_date)
            elif dob_year is not None:
                parsed_dob.append(dob_year)
            elif dob_type == "BETWEEN":
                from_year = dob["FROM_YEAR"]
                to_year = dob["TO_YEAR"]
                for year in range(int(from_year), int(to_year) + 1):
                    parsed_dob.append(str(year))
            elif dob_type is None:
                pass
            elif dob.get("NOTE") is not None:
                parsed_dob.append(dob["NOTE"])
            else:
                logger.error(
                    f"dob: {dob}. dob_date: {dob_date}. dob_year: {dob_year}")
                raise KeyError("dob listing does not have YEAR or DATE as key")

    else:
        pass

    return parsed_dob


def get_primary_name(profile_raw, source_program, source_profiletype_col):
    keys = ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME"]
    profile_type = profile_raw[source_profiletype_col]
    surname = ""
    if profile_type == "Individual":
        for key in reversed(keys):
            if profile_raw.get(key, None) is not None:
                surname = profile_raw[key].strip()
                break

    out_name = ""
    for key in keys:
        if profile_raw.get(key, None) is not None:
            out_name += " " + profile_raw[key].strip()
    out_name = out_name.strip()
    if (source_program == "KP") & (profile_type == "Individual"):
        out_name = swap_korea_name(out_name)

    title_value = profile_raw.get("TITLE", None)
    if title_value is not None:
        title_list = title_value["VALUE"]
        if type(title_list) is str:
            title = title_list.strip()
            out_name = title + " " + out_name
        else:
            out_list = []
            surname_list = []
            for title in title_list:
                out_list.append(title + " " + out_name)
                surname_list.append(surname)
            return out_list, surname_list
    else:
        pass

    return out_name.strip(), surname


def swap_korea_name(name):
    if type(name) is list:
        out_list = []
        for name_in in name:
            name_words = name_in.split(" ")
            first_word = [name_words[0]]
            other_word = name_words[1:]
            out_list.append(" ".join(other_word + first_word))
        return out_list
    else:
        name_words = name.split(" ")
        first_word = [name_words[0]]
        other_word = name_words[1:]
        return " ".join(other_word + first_word)


def get_alias_details(profile_raw, source_profiletype_col, source_program):
    parse_aka_list = []
    parse_lqaka_list = []
    parse_notes = []
    parse_aka_dob = []
    parse_surname_list = []

    profile_type = profile_raw[source_profiletype_col]
    if profile_type == "Entity":
        alias_key = "ENTITY_ALIAS"
    elif profile_type == "Individual":
        alias_key = "INDIVIDUAL_ALIAS"
    alias_entry_raw = profile_raw.get(alias_key, None)

    if alias_entry_raw is not None:
        if type(alias_entry_raw) is dict:
            alias_list = [alias_entry_raw]
        else:
            alias_list = alias_entry_raw

        for alias_entry in alias_list:
            if alias_entry["ALIAS_NAME"] is not None:
                if profile_type == "Individual":
                    alias_name_raw = format_name_individual(
                        alias_entry["ALIAS_NAME"])
                elif profile_type == "Entity":
                    alias_name_raw = alias_entry["ALIAS_NAME"]
                alias_name = alias_name_raw.split(";")
                if (source_program == "KP") & (profile_type == "Individual"):
                    alias_name = swap_korea_name(alias_name)
                alias_type = alias_entry["QUALITY"]
                if profile_type == "Entity":
                    surname = ["" for _ in alias_name]
                else:
                    surname = [name.split(",")[0].replace('"','').strip() if "," in name\
                        else "" for name in alias_name]
                if alias_type == "Good":
                    parse_aka_list += alias_name
                    parse_surname_list += surname
                elif alias_type == "Low":
                    parse_lqaka_list += alias_name
                else:
                    parse_aka_list += alias_name
                    parse_surname_list += surname
            else:
                pass

            alias_notes = alias_entry.get("NOTE", None)
            if alias_notes is not None:

                alias_notes_list = alias_notes.split(";")
                for alias_note in alias_notes_list:
                    parse_notes.append(alias_note.strip())

            alias_dob = alias_entry.get("DATE_OF_BIRTH", None)
            if alias_dob is not None:
                parse_aka_dob.append(alias_dob)

    return parse_aka_list, parse_lqaka_list, parse_notes, parse_aka_dob, parse_surname_list


def get_identity(profile_raw, source_profiletype_col):
    parsed_identity = []

    individual_id_raw = profile_raw.get("INDIVIDUAL_DOCUMENT", None)
    if individual_id_raw is not None:
        if type(individual_id_raw) is list:
            individual_id_list = individual_id_raw
        else:
            individual_id_list = [individual_id_raw]

        for individual_id in individual_id_list:
            if individual_id is not None:
                if individual_id.get("NUMBER", None) is not None:
                    id_number = individual_id["NUMBER"].strip()
                    parsed_identity.append(id_number)
                else:
                    for k, v in individual_id.items():
                        if k != "TYPE_OF_DOCUMENT":
                            parsed_identity.append(v.strip())

    return parsed_identity


def format_name_individual(name_raw):
    out = name_raw
    out = out.replace("(", ";")
    out = out.replace(")", "")

    return out
