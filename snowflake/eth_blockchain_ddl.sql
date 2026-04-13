create database if not exists eth_blockchain;
create schema if not exists eth_blockchain.gold;

use database eth_blockchain;

create table if not exists eth_blockchain.gold.blocks (
    partition_date              date,
    number                      integer,
    hash                        varchar primary key,
    miner                       varchar,
    difficulty                  double,
    total_difficulty            double,
    size                        integer,
    gas_limit                   integer,
    gas_used                    integer,
    base_fee_per_gas            integer,
    transaction_count           integer,
    timestamp                   timestamp,
    min_transaction_fee_percent double
);

create table if not exists eth_blockchain.gold.contracts (
    address  varchar primary key,
    bytecode varchar
);

create table if not exists eth_blockchain.gold.transactions (
    hash                          varchar,
    num_sender_prior_transactions integer,
    from_address                  varchar,
    to_address                    varchar,
    value                         double,
    gas                           integer,
    gas_price                     integer,
    receipt_gas_used              integer,
    max_priority_fee_per_gas      integer,
    max_fee_per_gas               integer,
    transaction_type              integer,
    receipt_status                integer,
    receipt_contract_address      varchar,
    block_hash                    varchar,
    foreign key (block_hash) references blocks (hash),
    foreign key (receipt_contract_address) references contracts (address)
);
