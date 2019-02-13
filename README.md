# Alchemy Table

Opinionated schema utilities for Google Bigtable and BigQuery.

Currently only schema generation functionality exists, but full ODM support is planned as well as BigQuery schema definition file and SQL view statement generation.

Built using the [Elixir Bigtable client library](https://github.com/bzzt/bigtable).

[![Hex.pm](https://img.shields.io/hexpm/v/alchemy_table.svg)](https://hex.pm/packages/alchemy_table)
[![Build Status](https://travis-ci.org/bzzt/alchemy_table.svg?branch=master)](https://travis-ci.org/bzzt/alchemy_table)
[![codecov](https://codecov.io/gh/bzzt/alchemy_table/branch/master/graph/badge.svg)](https://codecov.io/gh/bzzt/alchemy_table)
[![codebeat badge](https://codebeat.co/badges/3682f688-6bc9-401a-9a8c-ef7e038e0230)](https://codebeat.co/projects/github-com-bzzt-alchemy_table-master)
[![Built with Spacemacs](https://cdn.rawgit.com/syl20bnr/spacemacs/442d025779da2f62fc86c2082703697714db6514/assets/spacemacs-badge.svg)](http://spacemacs.org)

## Warning!

This is a work in progress and should not be used in production. Any and all functionality is subject to change or removal. The majority of functionality is not documented, and any documentation could be incorrect.

## Documentation

Documentation available at https://hexdocs.pm/bigtable/

## Installation

The package can be installed as:

```elixir
def deps do
 [{:alchemy_table, "~> 0.1.2"}]
end
```
