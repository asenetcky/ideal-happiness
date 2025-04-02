import data as d


def main():
    data = d.grab()
    # data = d.wrangle(data)
    d.write(data)


if __name__ == "__main__":
    main()
