import random
import string
import json

def generate_vendor_data(num_records=200, starting_vcode=123000, start_user_num=1):
    def random_pan():
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    def random_gst(pan_code):
        state_code = str(random.randint(10, 35))
        digit1 = str(random.randint(0,9))
        digit2 = str(random.randint(0,9))
        return f"{state_code}{pan_code}{digit1}Z{digit2}"

    def random_email(name):
        name_part = name.lower().replace(" ", ".")
        domains = ["example.com", "mail.com", "test.org"]
        return f"{name_part}@{random.choice(domains)}"

    data_list = []
    for i in range(num_records):
        vcode = f"{starting_vcode + i}"
        name = f"User{i+1} Lastname{i+1}"
        pan = random_pan()
        gst = random_gst(pan)
        email = random_email(name)

        record = {
            "vcode": vcode,
            "name": name,
            "pan": pan,
            "gst": gst,
            "email": email
        }
        data_list.append(record)
    return data_list

# Example usage:
if __name__ == "__main__":
    vendor_data = generate_vendor_data(200)
    print(json.dumps(vendor_data, indent=4))
