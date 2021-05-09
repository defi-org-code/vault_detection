from web3 import Web3
from tqdm import tqdm
import json
from pathlib import Path
import argparse
import csv
import math


class TopHolders(object):

    CONTRACT_INFO_FNAME = '{}/contracts_info.json'.format(str(Path().absolute()))
    CHUNK_SIZE = 4999
    N_BLOCKS = CHUNK_SIZE

    def __init__(self, verbose, eth_node_url, bsc_node_url):
        self.contract_info = None

        self.verbose = verbose
        self.eth_node_url = eth_node_url
        self.bsc_node_url = bsc_node_url

    def get_contracts_info(self):

        with open(self.CONTRACT_INFO_FNAME) as f:
            return json.load(f)

    @property
    def w3(self):

        contract_info = self.contract_info
        assert 'blockchain' in contract_info, 'Please add blockchain name to contract_info.json'
        blockchain = contract_info['blockchain']

        if blockchain.lower() == 'eth':
            return Web3(Web3.HTTPProvider(self.eth_node_url))
        elif blockchain.lower() == 'bsc':
            return Web3(Web3.HTTPProvider(self.bsc_node_url))
        else:
            raise TypeError('unsupported blockchain {}'.format(blockchain))

    @property
    def pid(self):

        contract_info = self.contract_info
        assert 'pid' in contract_info, 'Please add pid to contract_info.json'
        return contract_info['pid']

    @property
    def chunk_size(self):
        return self.contract_info.get('chunk_size') or self.CHUNK_SIZE

    @property
    def n_blocks(self):
        return self.contract_info.get('n_blocks') or self.N_BLOCKS

    @property
    def min_amount(self):
        return self.contract_info.get('min_amount') or 0

    @property
    def contract(self):
        return self.w3.eth.contract(address=Web3.toChecksumAddress(self.contract_info['address']), abi=self.contract_info['abi'])

    def get_contract(self, address, abi):
        return self.w3.eth.contract(address=address, abi=abi)

    def is_contract(self, addr):
        return self.w3.eth.getCode(addr) != b''

    @staticmethod
    def millify(n):
        millnames = ['', ' K', ' M', ' B', ' T']
        n = float(n)
        millidx = max(0,min(len(millnames)-1,
                            int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

        return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

    def csv_writer(self, row_names, data):

        if data:
            assert len(row_names) == len(data[0])

        if self.verbose >= 1:
            print('[{}][Write CSV] writing data to csv file'.format(self.contract_info['name']))

        with open('{}/{}.csv'.format(str(Path.home()), self.contract_info['name']), 'w') as out:

            csv_out = csv.writer(out)
            csv_out.writerow(row_names)

            for row in data:
                csv_out.writerow(row)

    def get_lp_ref_reserve(self, lp_contract):

        # get reserves
        lp_reserves = lp_contract.functions.getReserves().call()
        ref_token = self.contract_info['lp']['ref_token']

        assert ref_token < 2, 'ref_token should be 0 or 1 ({})'.format(ref_token)
        assert len(lp_reserves) == 3, 'unexpected length of getReserves ({})'.format(len(lp_reserves))

        return lp_reserves[ref_token]

    def get_master_chef_balance(self):
        lp_address, _, _, _ = self.contract.functions.poolInfo(self.pid).call()
        assert lp_address == self.contract_info['lp']['address'], 'please update lp address and abi in contract info (lp_address={})'.format(lp_address)
        lp_contract = self.get_contract(lp_address, self.contract_info['lp']['abi'])
        # get the balance of master-chef in lp contract
        master_chef_lp = lp_contract.functions.balanceOf(Web3.toChecksumAddress(self.contract_info['address'])).call()
        total_supply_lp = lp_contract.functions.totalSupply().call()

        lp_ref_reserve = self.get_lp_ref_reserve(lp_contract)
        norm_factor = self.contract_info['lp']['norm_factor']
        # master-chef balance in usd
        master_chef_balance_usd = 2 * lp_ref_reserve * (master_chef_lp / total_supply_lp) / norm_factor
        return master_chef_balance_usd, master_chef_lp

    @property
    def end_block(self):
        return self.w3.eth.blockNumber if 'end_block' not in self.contract_info else self.contract_info['end_block']

    def main(self):

        contracts_info = self.get_contracts_info()

        for i in range(len(contracts_info)):

            self.contract_info = contracts_info[i]
            if self.verbose >= 1:
                print('Running on contract {} ...'.format(self.contract_info['name']))

            if not self.contract_info['enabled']:
                if self.verbose >= 1:
                    print('{} is disabled, skipping contract\n'.format(self.contract_info['name']))
                continue

            to_block = self.end_block

            first_block = to_block - self.n_blocks
            from_block = to_block-self.chunk_size

            assert self.n_blocks >= self.chunk_size, 'n_blocks ({}) is expected to be >= chunk_size ({})'.format(self.n_blocks, self.chunk_size)

            all_deposits = dict()
            chunk_size = self.chunk_size

            total_pbar = to_block-first_block
            with tqdm(total=total_pbar) as pbar:

                pbar.set_description("[{}] Filter Deposits: ".format(self.contract_info['name']))

                while True:

                    if self.verbose >= 2:
                        print('chunk_size={}, from_block={}, to_block={}'.format(chunk_size, from_block, to_block))

                    deposit_filter = self.contract.events.Deposit.createFilter(
                        fromBlock=from_block, toBlock=to_block, argument_filters={'pid': self.pid})

                    try:
                        for entry in deposit_filter.get_all_entries():
                            if entry['args']['user'] not in all_deposits.keys():
                                all_deposits[entry['args']['user']] = entry['args']['amount']

                        if self.verbose >= 2:
                            print(all_deposits)

                    except Exception as e:

                        # TODO: rate limit, chunk too big  (e.args[0]['code'] == -32603)
                        if self.verbose >= 2:
                            print(e)

                        chunk_size = max(chunk_size // 2, 1)
                        from_block = to_block - chunk_size
                        continue

                    pbar.update(to_block-from_block)

                    to_block = from_block - 1
                    from_block = max(first_block, from_block-chunk_size)
                    chunk_size = self.chunk_size

                    if from_block <= first_block:
                        break

            pbar.close()
            _deposits = all_deposits

            master_chef_balance_usd, master_chef_lp = self.get_master_chef_balance()

            if self.verbose >= 2:
                print('master_chef_balance_usd = {}, master_chef_lp = {}'.format(master_chef_balance_usd, master_chef_lp))

            users_info = list()

            for addr in tqdm(list(_deposits.keys()), desc='[{}] Fetching User info: '.format(self.contract_info['name'])):

                amount, reward_debt = self.contract.functions.userInfo(self.pid, addr).call()

                if amount == 0:
                    continue

                users_info.append((addr, 100 * amount / master_chef_lp, self.millify(master_chef_balance_usd * amount / master_chef_lp),
                                   self.is_contract(addr)))

            if self.verbose >= 1:
                print('[{}] Sorting results by user info amount...'.format(self.contract_info['name']))

            users_info = sorted(users_info, key=lambda x: x[1], reverse=True)
            self.csv_writer(['address', 'amount_pct', 'balance_usd', 'is_contract'], users_info)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', required=False, help='[0, 1, 2] defaults to 1', default=1, type=int)
    parser.add_argument('-e', '--eth_node_url', required=False, help='ethereum node url', default='https://eth-mainnet.alchemyapi.io/v2/Obg4PgciCH3QtWqr_CYqYmkEEBc93SSo')
    parser.add_argument('-b', '--bsc_node_url', required=False, help='bsc node url', default='https://bsc-dataseed1.binance.org:443')
    args = parser.parse_args()

    top_holders = TopHolders(args.verbose, args.eth_node_url, args.bsc_node_url)
    top_holders.main()
