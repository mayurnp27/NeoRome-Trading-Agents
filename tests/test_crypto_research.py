import copy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import Mock
import urllib.request
from neorome.crypto_research import (BudgetOpener, Sources, research, discover, mint_checks,
                                    account_concentration, select_pool, trending_pool_observations, POLICY, TOKEN_PROGRAM, TOKEN_2022_PROGRAM)
from neorome.jupiter_quotes import QuoteError, SOL, USDC

MINT = '11111111111111111111111111111111'
NOW = datetime(2026,9,17,tzinfo=timezone.utc)


def mint_result():
    return {'context':{'slot':1000},'value':{'owner':TOKEN_PROGRAM,'executable':False,
        'data':{'parsed':{'type':'mint','info':{'isInitialized':True,'supply':'1000000',
        'decimals':6,'mintAuthority':None,'freezeAuthority':None}}}}}


def token_2022_mint_result():
    result = mint_result()
    result['value']['owner'] = TOKEN_2022_PROGRAM
    result['value']['data']['parsed']['info']['extensions'] = [
        {'extension': 'metadataPointer', 'state': {'authority': None, 'metadataAddress': MINT}},
        {'extension': 'tokenMetadata', 'state': {'updateAuthority': None, 'additionalMetadata': []}},
    ]
    return result


def largest_result():
    return {'context':{'slot':1001},'value':[{'address':SOL,'amount':'500000','decimals':6},
                                          {'address':USDC,'amount':'100000','decimals':6}]}


def pool():
    return {'chainId':'solana','baseToken':{'address':MINT},'quoteToken':{'address':SOL},'pairAddress':SOL,
            'liquidity':{'usd':100000},'pairCreatedAt':int(NOW.timestamp()*1000)-48*3600000}


def sources():
    src = Mock()
    src.profiles.return_value=([{'chainId':'solana','tokenAddress':MINT}],{'source':'fixture'})
    src.pairs.return_value=([pool()],{'source':'fixture'})
    src.rpc.side_effect=[(mint_result(),{'source':'fixture'}),(largest_result(),{'source':'fixture'})]
    return src


def quotes():
    client=Mock()
    client.quote.side_effect=[{'quote':{'outAmount':'100000'}},{'quote':{'outAmount':'4900000'}}]
    return client


class CryptoResearchTests(unittest.TestCase):
    def test_research_includes_trending_and_new_pool_observations_without_symbol_duplication(self):
        def payload(pool, liquidity):
            return {"data": [{"type": "pool", "id": "solana_" + pool,
                "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
                "attributes": {"base_token_price_usd": "1", "reserve_in_usd": liquidity,
                    "price_change_percentage": {"m5": "4", "h1": "6", "h6": "9"},
                    "volume_usd": {"m5": "3000", "h1": "12000"},
                    "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        src = sources()
        src.trending_pools.return_value = (payload(SOL, "30000"), {"source": "gecko", "received_at": NOW.isoformat(), "response_sha256": "a" * 64})
        src.new_pools.return_value = (payload(USDC, "40000"), {"source": "gecko", "received_at": NOW.isoformat(), "response_sha256": "b" * 64})
        result = research(src, quotes(), 1, NOW, include_trending=True)
        row = result["candidates"][0]
        self.assertEqual(row["mint"], MINT)
        self.assertEqual(len(row["trending_pool_observations"]), 2)
        self.assertEqual(row["trending_market"]["pool_address"], USDC)
        self.assertEqual(result["discovery_coverage"]["trending_pool_count"], 2)

    def test_gecko_trending_pool_projection_requires_chain_qualified_identity_and_preserves_causal_metrics(self):
        payload = {"data": [{"type": "pool", "id": "solana_" + SOL,
            "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
            "attributes": {"base_token_price_usd": "1.25", "reserve_in_usd": "30000",
                "price_change_percentage": {"m5": "4", "h1": "6", "h6": "9"},
                "volume_usd": {"m5": "3100", "h1": "12000"},
                "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        rows = trending_pool_observations(payload, {"source": "geckoterminal:solana-trending-pools",
            "received_at": NOW.isoformat(), "response_sha256": "a" * 64})
        self.assertEqual(rows[0]["tokenAddress"], MINT)
        self.assertEqual(rows[0]["trend_market"]["price_change_m5_percent"], "4")
        self.assertEqual(rows[0]["trend_market"]["volume_m5_usd"], "3100")
        self.assertEqual(rows[0]["trend_market"]["response_sha256"], "a" * 64)

    def test_gecko_trending_pool_projection_retains_signed_price_changes(self):
        payload = {"data": [{"type": "pool", "id": "solana_" + SOL,
            "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
            "attributes": {"base_token_price_usd": "1.25", "reserve_in_usd": "30000",
                "price_change_percentage": {"m5": "4", "h1": "-6", "h6": "-9"},
                "volume_usd": {"m5": "3100", "h1": "12000"},
                "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        rows = trending_pool_observations(payload, {"source": "geckoterminal:solana-trending-pools",
            "received_at": NOW.isoformat(), "response_sha256": "a" * 64})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["trend_market"]["price_change_h1_percent"], "-6")
        self.assertEqual(rows[0]["trend_market"]["price_change_h6_percent"], "-9")

    def test_gecko_trending_pool_projection_keeps_amount_fields_non_negative(self):
        payload = {"data": [{"type": "pool", "id": "solana_" + SOL,
            "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
            "attributes": {"base_token_price_usd": "1.25", "reserve_in_usd": "-30000",
                "price_change_percentage": {"m5": "4", "h1": "6", "h6": "9"},
                "volume_usd": {"m5": "3100", "h1": "12000"},
                "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        rows = trending_pool_observations(payload, {"source": "geckoterminal:solana-trending-pools",
            "received_at": NOW.isoformat(), "response_sha256": "a" * 64})
        self.assertEqual(rows, [])

    def test_discovery_deduplicates_filters_and_preserves_order(self):
        rows=[{'chainId':'ethereum','tokenAddress':MINT},{'chainId':'solana','tokenAddress':'https://evil.test'},
              {'chainId':'solana','tokenAddress':USDC},{'chainId':'solana','tokenAddress':MINT},
              {'chainId':'solana','tokenAddress':MINT},{'chainId':'solana','tokenAddress':TOKEN_PROGRAM}]
        self.assertEqual(discover(rows,2),[MINT,TOKEN_PROGRAM])

    def test_research_coverage_counts_raw_profiles_and_valid_unique_sol_mints(self):
        src = sources()
        src.profiles.return_value = ([
            {'chainId': 'solana', 'tokenAddress': MINT},
            {'chainId': 'solana', 'tokenAddress': MINT},
            {'chainId': 'solana', 'tokenAddress': 'not-a-mint'},
            {'chainId': 'ethereum', 'tokenAddress': MINT},
            {'chainId': 'solana', 'tokenAddress': USDC},
        ], {'source': 'fixture', 'response_sha256': 'a' * 64})
        coverage = research(src, quotes(), 1, NOW)['discovery_coverage']
        self.assertEqual(coverage['provider_profile_count'], 5)
        self.assertEqual(coverage['valid_unique_solana_mint_count'], 1)
        self.assertEqual(coverage['selected_count'], 1)
        self.assertEqual(coverage['interval_excluded_count'], 0)
        self.assertEqual(coverage['provider_provenance'][0]['response_sha256'], 'a' * 64)

    def test_research_selects_past_recent_mints_before_spending_candidate_budget(self):
        recent = MINT
        next_mint = TOKEN_PROGRAM
        src = sources()
        src.profiles.return_value = ([
            {'chainId': 'solana', 'tokenAddress': recent},
            {'chainId': 'solana', 'tokenAddress': next_mint},
        ], {'source': 'fixture', 'response_sha256': 'a' * 64})
        src.pairs.return_value = ([{
            'chainId': 'solana', 'baseToken': {'address': next_mint}, 'pairAddress': SOL,
            'liquidity': {'usd': 100000}, 'pairCreatedAt': int(NOW.timestamp() * 1000) - 48 * 3600000,
        }], {'source': 'fixture'})

        result = research(src, quotes(), 1, NOW, {recent})

        self.assertEqual([row['mint'] for row in result['candidates']], [next_mint])
        self.assertEqual(result['discovery_coverage']['valid_unique_solana_mint_count'], 2)
        self.assertEqual(result['discovery_coverage']['interval_excluded_count'], 1)
        src.pairs.assert_called_once_with(next_mint)

    def test_complete_observation_still_never_tradable(self):
        quote=quotes();data=research(sources(),quote,1,NOW);row=data['candidates'][0]
        self.assertEqual(row['research_status'],'observed_pending_risk_review')
        self.assertFalse(row['eligible_for_trading']);self.assertFalse(data['execution_enabled'])
        self.assertEqual(data['memecoin_allocation_usd'],0)
        self.assertEqual(quote.quote.call_args_list[0].args,(USDC,MINT,'4800000'))
        self.assertEqual(quote.quote.call_args_list[1].args,(MINT,USDC,'100000'))
        self.assertFalse(row['account_concentration']['beneficial_owners_identified'])
        self.assertEqual(row['account_concentration']['largest_account_supply_fraction'],'0.5')
        self.assertGreater(len(row['unresolved_checks']),0)

    def test_pool_filters_do_not_get_relaxed(self):
        for changes in ({'liquidity':{'usd':10}},{'pairCreatedAt':int(NOW.timestamp()*1000)-1000}):
            src=sources();pair=pool();pair.update(changes);src.pairs.return_value=([pair],{})
            quote=quotes();row=research(src,quote,1,NOW)['candidates'][0]
            self.assertEqual(row['research_status'],'screen_rejected')
            quote.quote.assert_not_called()

    def test_flexible_core_keeps_routes_when_strict_pool_metadata_is_absent(self):
        src = sources()
        pair = pool()
        del pair['pairAddress']
        del pair['pairCreatedAt']
        src.pairs.return_value = ([pair], {'source': 'dexscreener:token-pairs', 'response_sha256': 'b' * 64})
        src.rpc.side_effect = [
            (mint_result(), {'source': 'helius:getAccountInfo', 'response_sha256': 'c' * 64}),
            (largest_result(), {'source': 'helius:getTokenLargestAccounts', 'response_sha256': 'd' * 64}),
        ]
        quote = Mock()
        quote.quote.side_effect = [
            {'response_sha256': 'e' * 64, 'quote': {
                'inputMint': USDC, 'outputMint': MINT, 'inAmount': '2400000', 'outAmount': '100000',
                'otherAmountThreshold': '99000', 'priceImpact': '0.5', 'priceImpactUnit': 'percentage_points'}},
            {'response_sha256': 'f' * 64, 'quote': {
                'inputMint': MINT, 'outputMint': USDC, 'inAmount': '99000', 'outAmount': '4900000',
                'otherAmountThreshold': '4800000', 'priceImpact': '0.6', 'priceImpactUnit': 'percentage_points'}},
        ]

        row = research(src, quote, 1, NOW)['candidates'][0]

        self.assertEqual(row['research_status'], 'screen_rejected')
        self.assertIn('No complete matching pool metadata', row['screen_rejections'])
        self.assertIsNone(row['quotes'])
        self.assertEqual(row['flexible_core']['status'], 'collected_pending_cohort_assessment')
        self.assertEqual(row['flexible_core']['market_depth']['liquidity_usd'], '100000')
        self.assertEqual(row['flexible_core']['acquired_token_raw'], '99000')
        self.assertEqual(row['e3_position']['entry_input_usdc_raw'], '2400000')
        self.assertEqual(row['e3_entry_quote']['quote']['inAmount'], '2400000')
        self.assertEqual(set(row['flexible_core']['evidence_hashes']),
                         {'b' * 64, 'c' * 64, 'd' * 64, 'e' * 64, 'f' * 64})
        self.assertIn('No creator attribution or LP-vault exclusion is inferred from largest-account data',
                      row['flexible_core']['limitations'])

    def test_trending_depth_can_enrich_when_dex_mint_index_has_no_matching_pool(self):
        def payload(pool):
            return {"data": [{"type": "pool", "id": "solana_" + pool,
                "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
                "attributes": {"base_token_price_usd": "1", "reserve_in_usd": "30000",
                    "price_change_percentage": {"m5": "4", "h1": "6", "h6": "9"},
                    "volume_usd": {"m5": "3000", "h1": "12000"},
                    "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        src = sources()
        src.pairs.return_value = ([], {'source': 'dexscreener:token-pairs', 'response_sha256': 'b' * 64})
        src.trending_pools.return_value = (payload(SOL), {
            'source': 'geckoterminal:solana-trending-pools', 'received_at': NOW.isoformat(),
            'response_sha256': 'c' * 64})
        src.new_pools.return_value = ({'data': []}, {
            'source': 'geckoterminal:solana-new-pools', 'received_at': NOW.isoformat(),
            'response_sha256': 'd' * 64})
        src.rpc.side_effect = [
            (mint_result(), {'source': 'helius:getAccountInfo', 'response_sha256': 'e' * 64}),
            (largest_result(), {'source': 'helius:getTokenLargestAccounts', 'response_sha256': 'f' * 64}),
        ]
        quote = Mock()
        quote.quote.side_effect = [
            {'response_sha256': '1' * 64, 'quote': {
                'inputMint': USDC, 'outputMint': MINT, 'inAmount': '2400000', 'outAmount': '100000',
                'otherAmountThreshold': '99000', 'priceImpact': '-0.5', 'priceImpactUnit': 'percentage_points'}},
            {'response_sha256': '2' * 64, 'quote': {
                'inputMint': MINT, 'outputMint': USDC, 'inAmount': '99000', 'outAmount': '4900000',
                'otherAmountThreshold': '4800000', 'priceImpact': '0.6', 'priceImpactUnit': 'percentage_points'}},
        ]

        row = research(src, quote, 1, NOW, include_trending=True)['candidates'][0]

        self.assertIn('No complete matching pool metadata', row['screen_rejections'])
        self.assertEqual(row['flexible_core']['market_depth']['metadata_field_sources'],
                         {'liquidity_usd': 'geckoterminal.reserve_in_usd'})
        self.assertEqual(row['trend_position']['entry_input_usdc_raw'], '2400000')
        self.assertEqual(row['trend_entry_quote']['quote']['priceImpact'], '-0.5')
        self.assertIsNone(row['quotes'])

    def test_flexible_core_fails_closed_on_unproven_or_excessive_route_impact(self):
        src = sources()
        src.pairs.return_value = ([pool()], {'source': 'dexscreener:token-pairs', 'response_sha256': 'b' * 64})
        src.rpc.side_effect = [
            (mint_result(), {'source': 'helius:getAccountInfo', 'response_sha256': 'c' * 64}),
            (largest_result(), {'source': 'helius:getTokenLargestAccounts', 'response_sha256': 'd' * 64}),
        ]
        quote = Mock()
        quote.quote.side_effect = [
            {'response_sha256': 'e' * 64, 'quote': {
                'inputMint': USDC, 'outputMint': MINT, 'inAmount': '2400000', 'outAmount': '100000',
                'otherAmountThreshold': '99000', 'priceImpact': '2.01', 'priceImpactUnit': 'percentage_points'}},
            {'response_sha256': 'f' * 64, 'quote': {
                'inputMint': MINT, 'outputMint': USDC, 'inAmount': '99000', 'outAmount': '4900000',
                'otherAmountThreshold': '4800000', 'priceImpact': '0.6', 'priceImpactUnit': 'percentage_points'}},
        ]

        row = research(src, quote, 1, NOW)['candidates'][0]

        self.assertNotIn('flexible_core', row)
        self.assertEqual(row['flexible_collection_errors'], [{
            'stage': 'flexible_core', 'source': 'jupiter', 'kind': 'malformed_evidence',
            'reason': 'Malformed entry quote evidence'}])

    def test_missing_and_active_authorities_block_quotes(self):
        for field,value in [('mintAuthority',SOL),('freezeAuthority',SOL),('mintAuthority','missing')]:
            src=sources();mint=mint_result();info=mint['value']['data']['parsed']['info']
            if value=='missing': del info[field]
            else:info[field]=value
            src.rpc.side_effect=[(mint,{}),(largest_result(),{})]
            quote=quotes();row=research(src,quote,1,NOW)['candidates'][0]
            self.assertTrue(row['screen_rejections']);quote.quote.assert_not_called()

    def test_unsupported_program_never_inferred_safe(self):
        src=sources();mint=mint_result();mint['value']['owner']=USDC;src.rpc.side_effect=[(mint,{})]
        quote=quotes();row=research(src,quote,1,NOW)['candidates'][0]
        self.assertIn('unsupported_token_program',row['screen_rejections']);quote.quote.assert_not_called()
        self.assertEqual(src.rpc.call_count,1)

    def test_token_2022_only_allows_immutable_metadata_extensions(self):
        result=mint_result();result['value']['owner']=TOKEN_2022_PROGRAM
        result['value']['data']['parsed']['info']['extensions']=[
            {'extension':'metadataPointer','state':{'authority':None,'metadataAddress':MINT}},
            {'extension':'tokenMetadata','state':{'updateAuthority':None,'additionalMetadata':[]}}]
        info,findings=mint_checks(result)
        self.assertEqual(findings,[]);self.assertEqual(info['reviewed_extensions'],['metadataPointer','tokenMetadata'])
        result['value']['data']['parsed']['info']['extensions'][0]['state']['authority']=SOL
        self.assertIn('metadata_pointer_authority_present',mint_checks(result)[1])

    def test_token_2022_with_reviewed_immutable_extensions_gets_trend_projection(self):
        def payload():
            return {"data": [{"type": "pool", "id": "solana_" + SOL,
                "relationships": {"base_token": {"data": {"id": "solana_" + MINT}}},
                "attributes": {"base_token_price_usd": "1", "reserve_in_usd": "30000",
                    "price_change_percentage": {"m5": "4", "h1": "6", "h6": "9"},
                    "volume_usd": {"m5": "3000", "h1": "12000"},
                    "transactions": {"m5": {"buys": 12, "sells": 6}}}}]}
        src = sources()
        src.pairs.return_value = ([], {
            'source': 'dexscreener:token-pairs', 'response_sha256': 'b' * 64})
        src.trending_pools.return_value = (payload(), {
            'source': 'geckoterminal:solana-trending-pools', 'received_at': NOW.isoformat(),
            'response_sha256': 'a' * 64})
        src.new_pools.return_value = ({'data': []}, {
            'source': 'geckoterminal:solana-new-pools', 'received_at': NOW.isoformat(),
            'response_sha256': 'b' * 64})
        src.rpc.side_effect = [
            (token_2022_mint_result(), {'source': 'helius:getAccountInfo', 'response_sha256': 'c' * 64}),
            (largest_result(), {'source': 'helius:getTokenLargestAccounts', 'response_sha256': 'd' * 64}),
        ]
        quote = Mock()
        quote.quote.side_effect = [
            {'response_sha256': 'e' * 64, 'quote': {
                'inputMint': USDC, 'outputMint': MINT, 'inAmount': '2400000', 'outAmount': '100000',
                'otherAmountThreshold': '99000', 'priceImpact': '-0.5', 'priceImpactUnit': 'percentage_points'}},
            {'response_sha256': 'f' * 64, 'quote': {
                'inputMint': MINT, 'outputMint': USDC, 'inAmount': '99000', 'outAmount': '4900000',
                'otherAmountThreshold': '4800000', 'priceImpact': '0.6', 'priceImpactUnit': 'percentage_points'}},
        ]

        row = research(src, quote, 1, NOW, include_trending=True)['candidates'][0]

        self.assertEqual(row['mint_checks']['program'], TOKEN_2022_PROGRAM)
        self.assertEqual(row['mint_checks']['reviewed_extensions'], ['metadataPointer', 'tokenMetadata'])
        self.assertEqual(row['trend_token_controls']['program'], TOKEN_2022_PROGRAM)
        self.assertEqual(row['trend_position']['entry_input_usdc_raw'], '2400000')

    def test_token_2022_unreviewed_or_mutable_controls_do_not_get_trend_projection(self):
        from neorome.crypto_research import _trend_projection_allowed

        safe = {'program': TOKEN_2022_PROGRAM, 'mintAuthority': None, 'freezeAuthority': None,
                'reviewed_extensions': ['metadataPointer', 'tokenMetadata']}
        self.assertTrue(_trend_projection_allowed(safe))
        for unsafe in (
            dict(safe, reviewed_extensions=['metadataPointer']),
            dict(safe, reviewed_extensions=['metadataPointer', 'tokenMetadata', 'transferHook']),
            dict(safe, mintAuthority=SOL),
            dict(safe, freezeAuthority=SOL),
        ):
            self.assertFalse(_trend_projection_allowed(unsafe))

    def test_missing_largest_accounts_and_rpc_failure_fail_closed(self):
        for outcome in [( {'context':{'slot':1001},'value':[]}, {}),QuoteError('Research provider HTTP 401')]:
            src=sources();src.rpc.side_effect=[(mint_result(),{}),outcome]
            quote=quotes();row=research(src,quote,1,NOW)['candidates'][0]
            self.assertEqual(row['research_status'],'screen_rejected');quote.quote.assert_not_called()

    def test_concentration_rejects_inconsistent_snapshots(self):
        info,_=mint_checks(mint_result())
        for change in ('supply','duplicate','skew','decimals'):
            result=largest_result()
            if change=='supply':result['value'][0]['amount']='1000001'
            if change=='duplicate':result['value'][1]['address']=SOL
            if change=='skew':result['context']['slot']=2000
            if change=='decimals':result['value'][0]['decimals']=True
            with self.assertRaises(QuoteError):account_concentration(result,info)

    def test_pool_metadata_missing_or_nonfinite_does_not_pass(self):
        for bad in (None,float('nan'),float('inf'),'not a number'):
            pair=pool();pair['liquidity']['usd']=bad
            with self.assertRaises(QuoteError):select_pool([pair],MINT,NOW)
        pair=pool();pair['pairCreatedAt']=int(NOW.timestamp()*1000)+100
        with self.assertRaises(QuoteError):select_pool([pair],MINT,NOW)

    def test_pool_normalizes_documented_equivalent_fields_and_quote_side_match(self):
        pair = pool()
        pair['baseToken'], pair['quoteToken'] = {'address': SOL}, {'address': MINT}
        pair['poolAddress'] = pair.pop('pairAddress')
        pair['liquidityUsd'] = pair.pop('liquidity')['usd']
        pair['createdAt'] = pair.pop('pairCreatedAt')
        selected = select_pool([pair], MINT, NOW)
        self.assertEqual(selected['matched_token_side'], 'quote')
        self.assertEqual(selected['counter_token_address'], SOL)
        self.assertEqual(selected['metadata_field_sources'], {
            'pair_address': 'poolAddress', 'liquidity_usd': 'liquidityUsd', 'pair_created_at_ms': 'createdAt'})

    def test_pool_conflicting_equivalent_metadata_fails_closed(self):
        pair = pool(); pair['poolAddress'] = USDC
        with self.assertRaises(QuoteError): select_pool([pair], MINT, NOW)

    def test_malformed_quote_evidence_is_rejected_with_bounded_collection_error(self):
        quote = quotes()
        quote.quote.side_effect = [{'quote': {'outAmount': '100000', 'otherAmountThreshold': '100001'}}]
        row = research(sources(), quote, 1, NOW)['candidates'][0]
        self.assertEqual(row['research_status'], 'screen_rejected')
        self.assertIsNone(row['quotes'])
        self.assertEqual(row['collection_errors'], [{
            'stage': 'entry_preflight', 'source': 'jupiter', 'kind': 'malformed_evidence',
            'reason': 'Malformed entry quote evidence'}])
        quote.quote.assert_called_once_with(USDC, MINT, '4800000')

    def test_e2_reuses_only_qualified_pool_and_hash_bound_existing_routes(self):
        src = sources()
        pair = pool()
        pair.update({'volume': {'h1': '25000'}, 'txns': {'h1': {'buys': 20, 'sells': 12}}})
        src.profiles.return_value = ([{'chainId': 'solana', 'tokenAddress': MINT}],
                                     {'source': 'fixture', 'response_sha256': 'a' * 64})
        src.pairs.return_value = ([pair], {'source': 'dexscreener:token-pairs', 'response_sha256': 'b' * 64})
        src.rpc.side_effect = [(mint_result(), {'source': 'helius:getAccountInfo', 'response_sha256': 'c' * 64}),
                               (largest_result(), {'source': 'helius:getTokenLargestAccounts', 'response_sha256': 'd' * 64})]
        quote = quotes()
        quote.quote.side_effect = [
            {'response_sha256': 'e' * 64, 'quote': {'outAmount': '100000'}},
            {'response_sha256': 'f' * 64, 'quote': {'outAmount': '4900000'}},
            {'response_sha256': '1' * 64, 'quote': {
                'inputMint': USDC, 'outputMint': MINT, 'inAmount': '2400000', 'outAmount': '80000',
                'otherAmountThreshold': '75000', 'priceImpact': '0.5', 'priceImpactUnit': 'percentage_points'}},
            {'response_sha256': '2' * 64, 'quote': {
                'inputMint': MINT, 'outputMint': USDC, 'inAmount': '75000', 'outAmount': '2400000',
                'otherAmountThreshold': '2300000', 'priceImpact': '0.5', 'priceImpactUnit': 'percentage_points'}},
        ]
        row = research(src, quote, 1, NOW)['candidates'][0]
        self.assertEqual(row['e2_pool']['source'], 'dexscreener:token-pairs')
        self.assertEqual(row['e2_pool']['liquidity_usd'], '100000')
        self.assertEqual(row['e2_costs']['entry_input_usdc_raw'], '4800000')
        self.assertEqual(row['e2_acquired_token_raw'], '100000')
        self.assertEqual(row['e2_sell_preflight']['response_sha256'], 'f' * 64)
        self.assertEqual(set(row['e2_evidence_hashes']), {'b' * 64, 'c' * 64, 'd' * 64, 'e' * 64, 'f' * 64})
        self.assertNotIn('e2_holder_controls', row)
        self.assertEqual(row['e3_position']['entry_input_usdc_raw'], '2400000')

    def test_invalid_limit_causes_no_provider_calls(self):
        for count in (0,4,True):
            src=sources()
            with self.assertRaises(QuoteError):research(src,quotes(),count,NOW)
            src.profiles.assert_not_called()

    def test_budget_counts_failures_and_denies_write_methods_and_hosts(self):
        opener=Mock();opener.open.side_effect=OSError('failure')
        budget=BudgetOpener(1,opener,clock=lambda:0,sleep=Mock())
        req=urllib.request.Request('https://api.dexscreener.com/token-profiles/latest/v1')
        with self.assertRaises(OSError):budget.open(req)
        with self.assertRaises(QuoteError):budget.open(req)
        self.assertEqual(budget.used,1);self.assertEqual(opener.open.call_count,1)
        for url,data in [('https://evil.test/',None),('https://api.jup.ag/swap/v2/execute',b'{}'),
                         ('https://mainnet.helius-rpc.com/',json.dumps({'method':'sendTransaction'}).encode())]:
            fresh=BudgetOpener(6,Mock())
            with self.assertRaises(QuoteError):fresh.open(urllib.request.Request(url,data=data))
            self.assertEqual(fresh.used,0)

    def test_sources_only_allow_read_rpc_and_sanitize_transport_errors(self):
        opener=Mock();loader=Mock(return_value='not-a-real-secret');src=Sources(opener,loader)
        with self.assertRaises(QuoteError):src.rpc('sendTransaction',MINT)
        loader.assert_not_called();opener.open.assert_not_called()
        opener.open.side_effect=RuntimeError('https://mainnet.helius-rpc.com/?api-key=not-a-real-secret')
        with self.assertRaises(QuoteError) as caught:src.rpc('getAccountInfo',MINT)
        self.assertNotIn('not-a-real-secret',str(caught.exception))


if __name__=='__main__':unittest.main()
