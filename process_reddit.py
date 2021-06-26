"""
Matches scraped reddit posts to stock tickers to mark which reddit post is about which stock. 
"""
import pandas as pd


def create_hitlist(row):
    """
    For each company id, creates list of strings to be looked-up within a reddit post
    e.g., for comapny_id "AAPL", hitlist is
    ["$AAPL ", "$AAPL,", "[AAPL]", "(AAPL)", "($AAPL), [$AAPL]"]
    """
    hitlist = []
    hitlist.append(f"${row.company_id} ")
    hitlist.append(f"${row.company_id},")
    hitlist.append(f"$[{row.company_id}]")
    hitlist.append(f"({row.company_id})")
    hitlist.append(f"(${row.company_id})")
    hitlist.append(f"[${row.company_id}]")
    # hitlist.append(f" {row.company_id} ")
    # name_first_part = row.company_name.split(",")[:1][0]
    # name_first_part = name_first_part.split()[0]
    # hitlist.append(f"{name_first_part} ")
    return hitlist


if __name__ == "__main__":
    # Load tickers and reddit data, process them a little bit
    tickers = pd.read_csv("data/tickers_processed.csv", index_col=0)
    reddit = pd.read_csv("data/reddit.csv", index_col=0)
    reddit["date"] = pd.to_datetime(reddit["date"])
    reddit = reddit[reddit["date"] >= pd.to_datetime("2021-05-25")]
    reddit["body"] = reddit["body"].fillna("")
    reddit["body"] = reddit["body"].astype(str)
    tickers["company_name"] = tickers["name"]

    # For each company id, creates list of strings to be looked-up within a reddit post ("hits")
    tickers["hitlist"] = tickers.apply(create_hitlist, axis=1)

    # create dict that converts company ids to hits
    # e.g. {"AAPL": ["$AAPL", "[AAPL]"]}
    id_to_hitlist = pd.Series(
        tickers.hitlist.values, index=tickers.company_id
    ).to_dict()

    # create dict that converts hits to company ids
    # e.g. {"$AAPL": ["AAPL"], "[AAPL]:["AAPL"]"}
    hit_to_idlist = dict()
    for k, val in id_to_hitlist.items():
        for v in val:
            if v in hit_to_idlist:
                hit_to_idlist[v] = hit_to_idlist[v] + [k]
            else:
                hit_to_idlist[v] = [k]

    # create a list of all hits
    hitlist = tickers.hitlist.values.tolist()
    hitlist = [item for sublist in hitlist for item in sublist]  # flatten list
    hitlist = list(set(hitlist))

    # clean up the list
    # remove reddit guideline to use proper symbol notation
    # \n\n**Use $SYMBOL FORMAT**  ($BB or $[BB.TO](https://BB.TO))'
    # remove (DD]) which marks Due Dilligence
    hitlist = [e for e in hitlist if e not in ["$BB ", "$[BB.TO]", "(DD)", "[DD]"]]

    # LOOKUP IN TITLE ONLY
    # lookup through reddit posts and find hits in post titles
    # reddit["company_id"] = reddit.apply(
    #    lambda x: [hit_to_idlist.get(hit) for hit in hitlist if hit in x["title"]],
    #    axis=1,
    # )

    # LOOKUP THROUGH TITLE AND BODY
    # lookup through reddit posts and find hits in post titles or body
    reddit["company_id"] = reddit.apply(
        lambda x: [
            hit_to_idlist.get(hit)
            for hit in hitlist
            if (hit in x["title"] or hit in x["body"])
        ],
        axis=1,
    )

    # flatten the list
    reddit["company_id"] = reddit["company_id"].apply(
        lambda x: [item for sublist in x for item in sublist]
    )

    # drop posts with no hits
    reddit = reddit[reddit.company_id.str.len() != 0]

    # reformat table so that each reddit posts
    # (in case that a single post correponds to more company_ids)
    reddit = reddit.explode("company_id")

    # save
    reddit.to_csv("data/reddit_processed_title_body.csv")
