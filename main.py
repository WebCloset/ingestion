from ebay import fetch_ebay
from reverb import fetch_reverb
from normalize import deduplication

def main():
    print("Starting all ingestion jobs...")

    try:
        fetch_ebay.main()
    except Exception as e:
        print("Failed to fetch ebay data : ",e)


    try:
        fetch_reverb.main()
    except Exception as e:
        print("Failed to fetch reverb data : ",e)

    try:
        deduplication.main()
    except Exception as e:
        print("Failed to deduplicate data : ",e)

    print("All ingestion jobs completed")

if __name__ == "__main__":
    main()