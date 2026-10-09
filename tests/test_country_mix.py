"""
The 80% U.S. rule (operator instruction 2026-09-30).

Each niche's run writes at most MAX_NON_PRIORITY_COUNTRY_SHARE (20%) of its rows
from outside the U.S. CountryMix counts across every batch of the niche; a
qualifying non-U.S. channel that doesn't fit is held (DROP_COUNTRY_MIX, never
cached as rejected) and retried at the end of the niche.
"""
import inspect

from channel_vetting import config, pipeline
from channel_vetting.ranking.scoring import QUALIFIED


def test_the_live_split_is_80_20_us():
    assert config.PRIORITY_COUNTRY_CODE == "US"
    assert config.MAX_NON_PRIORITY_COUNTRY_SHARE == 0.20


def _written(mix, country, n=1):
    for i in range(n):
        cid = f"{country}{mix.priority_rows + mix.other_rows}-{i}"
        mix.note(cid, country)
        mix.record(cid)


def test_a_non_us_row_needs_four_us_rows_ahead_of_it():
    mix = pipeline.CountryMix()
    assert mix.allows("GB") is False
    _written(mix, "US", 3)
    assert mix.allows("CA") is False
    _written(mix, "US")
    assert mix.allows("CA") is True
    _written(mix, "CA")
    assert mix.allows("AU") is False
    assert mix.allows("US") is True


def test_the_ratio_never_exceeds_20_percent():
    mix = pipeline.CountryMix()
    for n in range(1, 60):
        _written(mix, "US")
        while mix.allows("GB"):
            _written(mix, "GB")
        total = mix.priority_rows + mix.other_rows
        assert mix.other_rows / total <= 0.20 + 1e-9, (n, mix.other_rows, total)
    # And it is not stricter than asked: 59 U.S. rows admit 14 others.
    assert mix.other_rows == 59 // 4


def test_country_names_count_as_their_code():
    mix = pipeline.CountryMix()
    _written(mix, "United States", 4)
    assert mix.priority_rows == 4


def test_only_rows_actually_written_are_counted():
    mix = pipeline.CountryMix()
    mix.note("UC1", "US")          # passed the gate, then the push failed
    assert mix.priority_rows == 0
    mix.record("UC_unknown")       # never noted: ignored
    assert (mix.priority_rows, mix.other_rows) == (0, 0)


def test_share_of_one_switches_the_rule_off():
    assert pipeline.CountryMix(max_other_share=1.0).allows("GB") is True


def _builder(countries, paid):
    """A build_record that mirrors process_candidate's order: country check,
    then spend."""
    def build(candidate, has_room):
        mix = candidate["mix"]
        if not mix.allows(countries[candidate["id"]]):
            return None, pipeline.DROP_COUNTRY_MIX
        if not has_room(QUALIFIED):
            return None, pipeline.DROP_NO_HEADROOM
        mix.note(candidate["id"], countries[candidate["id"]])
        paid.append(candidate["id"])
        return {"Channel ID": candidate["id"]}, QUALIFIED
    return build


def test_push_until_full_holds_non_us_and_never_caches_them(monkeypatch):
    monkeypatch.setattr(pipeline, "push_record", lambda t, rec: True)
    mix = pipeline.CountryMix()
    countries = {"UC0": "GB", "UC1": "US", "UC2": "US", "UC3": "US", "UC4": "US"}
    cands = [{"id": k, "mix": mix, "handle": f"@{k.lower()}"} for k in countries]
    paid = []

    counts = pipeline.push_until_full(
        cands, _builder(countries, paid), "tbl", 10, country_mix=mix,
    )

    assert counts["qualified"] == 4
    assert paid == ["UC1", "UC2", "UC3", "UC4"]     # no email spend on the held one
    assert [c["id"] for c in mix.deferred] == ["UC0"]
    assert counts["rejected_handles"] == set()      # transient, not a rejection
    assert (mix.priority_rows, mix.other_rows) == (4, 0)
    # Four U.S. rows now make room for exactly the held one.
    assert mix.other_room() == 1


def test_country_mix_is_checked_before_the_email_chain():
    src = inspect.getsource(pipeline.process_candidate)
    assert src.index("country_mix.allows(") < src.index("= resolve_email_with_source("), (
        "the 80/20 check must run BEFORE the paid email chain"
    )


def test_country_mix_drop_is_transient():
    assert pipeline.DROP_COUNTRY_MIX in pipeline.TRANSIENT_DROP_REASONS


def _run_niche(monkeypatch, countries):
    pushed, paid = [], []
    calls = {"n": 0}

    def fake_discovery(batch, **kw):
        calls["n"] += 1
        return [{"channel_id": k} for k in countries] if calls["n"] == 1 else []

    def fake_process(candidate, *a, has_room=None, country_mix=None, **kw):
        cid = candidate["channel_id"]
        if not country_mix.allows(countries[cid]):
            return None, pipeline.DROP_COUNTRY_MIX
        if not has_room(QUALIFIED):
            return None, pipeline.DROP_NO_HEADROOM
        country_mix.note(cid, countries[cid])
        paid.append(cid)
        return {"Channel ID": cid}, QUALIFIED

    monkeypatch.setattr(pipeline, "count_added_today", lambda table, qualification=None: 0)
    monkeypatch.setattr(pipeline, "get_tracked_handles", lambda table: set())
    monkeypatch.setattr(pipeline, "run_discovery", fake_discovery)
    monkeypatch.setattr(pipeline, "process_candidate", fake_process)
    monkeypatch.setattr(pipeline, "push_record", lambda t, rec: pushed.append(rec["Channel ID"]) or True)

    cfg = dict(pipeline.NICHES["Lifestyle Sofa"], discovery_source="search_list")
    pipeline.run_niche(
        "Lifestyle Sofa", "tbl", ["kw1", "kw2"], 50, 7, set(), {}, None,
        cfg, None, None, None,
    )
    return pushed, paid


def test_run_niche_retries_a_held_non_us_channel_at_the_end(monkeypatch):
    """End to end over run_niche's keyword path: a U.K. channel found FIRST is
    held, the four U.S. rows after it land, and the end-of-niche retry then
    writes it — 4 U.S. / 1 other, exactly 80/20."""
    pushed, _ = _run_niche(monkeypatch, {
        "UC0": "GB", "UC1": "US", "UC2": "US", "UC3": "US", "UC4": "US",
    })
    assert pushed == ["UC1", "UC2", "UC3", "UC4", "UC0"]


def test_run_niche_never_writes_more_than_its_share(monkeypatch):
    """Two others and four U.S.: the AU channel arriving after the U.S. rows
    takes the one open slot, and the held U.K. channel is left for a later run
    without an email lookup."""
    pushed, paid = _run_niche(monkeypatch, {
        "UC0": "GB", "UC1": "US", "UC2": "US", "UC3": "US", "UC4": "US",
        "UC5": "AU",
    })
    assert pushed == ["UC1", "UC2", "UC3", "UC4", "UC5"]
    assert "UC0" not in paid
