# Remaining Host Review - Comprehensive Proposal

Generated: 2026-04-29T06:04:27.085283+00:00

## Scope

- Unique host values reviewed: 2,149
- Total review-needed rows represented: 3,156
- File read issues: 0

## Proposed Decision Summary

| Decision | Unique values | Rows represented |
|---|---:|---:|
| manual_review_remaining | 1,190 | 1,555 |
| taxonomy_exact_lookup | 718 | 1,113 |
| not_identifiable_or_missing | 137 | 138 |
| broad_host_group | 64 | 227 |
| non_host_lab_or_person | 19 | 33 |
| non_host_source | 18 | 27 |
| non_host_microbe_culture_descriptor | 2 | 2 |
| probable_broad_host_group | 1 | 61 |

## Apply Recommendation Summary

| Recommendation | Unique values |
|---|---:|
| manual_validation_required | 1,191 |
| safe_to_apply | 727 |
| review_before_apply | 213 |
| safe_to_apply_if_destination_empty | 18 |

## Highest Impact Values

| Count | Host original | Proposed decision | Proposed Host_SD | TaxID | Recommendation | Reasoning |
|---:|---|---|---|---|---|---|
| 61 | Tique | probable_broad_host_group | Ixodida | 6935 | manual_validation_required | Tique is likely a tick term in some languages, but this is not safe enough for automatic mapping. |
| 11 | Antho dichotoma | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 6 | uncultured eukaryote | broad_host_group | Eukaryota | 2759 | review_before_apply | broad eukaryote host |
| 6 | flower | broad_host_group | Viridiplantae | 33090 | review_before_apply | plant material; host not taxonomically specific |
| 6 | Bathyopsurus nybelini specimen AT50-02-018 | taxonomy_exact_lookup | bathyopsurus nybelini | 3120861 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 6 | Sprague Dawley | broad_host_group | Rattus norvegicus | 10116 | review_before_apply | laboratory rat strain |
| 6 | Edessa sp. | taxonomy_exact_lookup | edessa | 1225063 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 6 | protist | broad_host_group | Eukaryota | 2759 | review_before_apply | broad protist/eukaryote host |
| 6 | Coffea | taxonomy_exact_lookup | coffea | 13442 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 5 | N. Ennis, Tisa lab UNH | non_host_lab_or_person |  |  | safe_to_apply | Value appears to be a collector, laboratory, institute, or facility rather than a biological host. |
| 5 | Beaked hazelnut | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 5 | Acanthamoeba | taxonomy_exact_lookup | acanthamoeba | 5754 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 5 | marine macroalgae | broad_host_group | Viridiplantae | 33090 | review_before_apply | broad algal/plant host |
| 5 | nestling stork | broad_host_group | Aves | 8782 | review_before_apply | broad bird host |
| 5 | Skeletonema marinoi strain RO5AC | taxonomy_exact_lookup | skeletonema marinoi | 267567 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 5 | seabird | broad_host_group | Aves | 8782 | review_before_apply | broad bird host |
| 5 | Porifera | broad_host_group | Porifera | 6040 | review_before_apply | sponge host |
| 5 | Pseudomonas | taxonomy_exact_lookup | pseudomonas | 286 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 5 | Testudines | broad_host_group | Testudines | 8459 | review_before_apply | turtle/tortoise host |
| 5 | Pogona | broad_host_group | Pogona | 103695 | review_before_apply | bearded dragon genus host |
| 5 | Instituto de Productos Lacteos de Asturias (IPLA)-CSIC | non_host_lab_or_person |  |  | safe_to_apply | Value appears to be a collector, laboratory, institute, or facility rather than a biological host. |
| 4 | Soricidae | taxonomy_exact_lookup | soricidae | 9376 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | sturgeon | broad_host_group | Acipenseridae | 7902 | review_before_apply | sturgeon host |
| 4 | Sablefish | broad_host_group | Anoplopoma fimbria | 229290 | review_before_apply | sablefish host |
| 4 | Sedum | taxonomy_exact_lookup | sedum | 3784 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | carrot | taxonomy_exact_lookup | carrot | 4039 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | macaque | taxonomy_exact_lookup | macaque | 9539 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | macroalgae | broad_host_group | Viridiplantae | 33090 | review_before_apply | broad algal/plant host |
| 4 | Finnraccoon | broad_host_group | Nyctereutes procyonoides | 34880 | review_before_apply | finnraccoon/finnish raccoon dog host |
| 4 | Prof. Gail Preston | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Ginger | taxonomy_exact_lookup | ginger | 94328 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Chimpanzee | taxonomy_exact_lookup | chimpanzee | 9598 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Gryllotalpidae | taxonomy_exact_lookup | gryllotalpidae | 208677 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | elk | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Hexactinellid | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Bradypus | taxonomy_exact_lookup | bradypus | 9353 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Common chimpanzee | broad_host_group | Pan troglodytes | 9598 | review_before_apply | chimpanzee host |
| 4 | Leobordea sp. | taxonomy_exact_lookup | leobordea | 1096780 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Oxytropis sp. | taxonomy_exact_lookup | oxytropis | 20802 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Laridae | taxonomy_exact_lookup | laridae | 8910 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Beach Morning Glory Ipomoea pes-caprae | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Wader | broad_host_group | Aves | 8782 | review_before_apply | broad bird host |
| 4 | Ulva | taxonomy_exact_lookup | ulva | 3118 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | mungbean | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | grasshopper | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Blattoidea | taxonomy_exact_lookup | blattoidea | 1049657 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | rook | taxonomy_exact_lookup | rook | 75140 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Ostrich | taxonomy_exact_lookup | ostrich | 8801 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Rhodnius prolixus 5th instar | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Southern caracara | broad_host_group | Aves | 8782 | review_before_apply | bird host; species-level TaxonKit exact lookup unavailable |
| 4 | day-old chicks | broad_host_group | Gallus gallus | 9031 | review_before_apply | domestic chicken host |
| 4 | Agouti Lowlan paca | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Oecomys cf bicolor | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Golden Threadfin Bream | taxonomy_exact_lookup | golden threadfin bream | 450225 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | okapi | taxonomy_exact_lookup | okapi | 86973 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | common eider | taxonomy_exact_lookup | common eider | 76058 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | king eider | taxonomy_exact_lookup | king eider | 203439 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Crested Tern | broad_host_group | Thalasseus bergii | 1843445 | review_before_apply | crested tern host |
| 4 | Ox | broad_host_group | Bos taurus | 9913 | review_before_apply | cattle host |
| 4 | Ape | broad_host_group | Hominoidea | 314295 | review_before_apply | broad ape host; avoids ambiguous plant taxon named ape |
| 4 | whale | taxonomy_exact_lookup | whale | 9721 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Bullfinch | broad_host_group | Aves | 8782 | review_before_apply | bird host; common name is not species-specific |
| 4 | Vulture | broad_host_group | Aves | 8782 | review_before_apply | broad vulture/bird host |
| 4 | Oregon Junco | broad_host_group | Junco | 40209 | review_before_apply | junco host; subspecies/common-name resolution should be validated |
| 4 | owl | broad_host_group | Strigiformes | 30472 | review_before_apply | broad owl host |
| 4 | D. marginatus | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Possum | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Plantain | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Alhagi sparsifolia Shap. | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | coffe tree | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Agile Gibbon | taxonomy_exact_lookup | agile gibbon | 9579 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Chiu-Chung Young | non_host_lab_or_person |  |  | safe_to_apply | Value appears to be a collector, laboratory, institute, or facility rather than a biological host. |
| 4 | Mortierellaceae | taxonomy_exact_lookup | mortierellaceae | 4854 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | scallop | broad_host_group | Pectinidae | 6570 | review_before_apply | scallop host |
| 4 | melon | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Dasyurus sp. | taxonomy_exact_lookup | dasyurus | 9278 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Medicinal Cannabis | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Lepidoptera | taxonomy_exact_lookup | lepidoptera | 7088 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Parsley | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | tea | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | artichoke | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | muskmelon | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Poa | taxonomy_exact_lookup | poa | 4544 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Glycine | taxonomy_exact_lookup | glycine | 3846 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | clover | broad_host_group | Viridiplantae | 33090 | review_before_apply | Common plant/material term; useful as broad plant host but not species-level. |
| 4 | Fuchsia | taxonomy_exact_lookup | fuchsia | 13069 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Oenothera | taxonomy_exact_lookup | oenothera | 3939 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Erysimum | taxonomy_exact_lookup | erysimum | 65352 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | cardinal | taxonomy_exact_lookup | cardinal | 405018 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | crane | broad_host_group | Gruidae | 9109 | review_before_apply | broad crane host |
| 4 | Pogona sp. | broad_host_group | Pogona | 103695 | review_before_apply | bearded dragon genus host |
| 4 | gecko | broad_host_group | Gekkota | 8509 | review_before_apply | broad gecko host |
| 4 | pogona | broad_host_group | Pogona | 103695 | review_before_apply | bearded dragon genus host |
| 4 | Taurus | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | tiapia | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Clodonia rei | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Seriola | taxonomy_exact_lookup | seriola | 8160 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | S. aurata | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 4 | Kelp | broad_host_group | Phaeophyceae | 2870 | review_before_apply | kelp/brown algae host |
| 4 | Philodendron | taxonomy_exact_lookup | philodendron | 71613 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Corylus | taxonomy_exact_lookup | corylus | 13450 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 4 | Chacoan mara | broad_host_group | Dolichotis salinicola | 210422 | review_before_apply | chacoan mara host |
| 3 | Aachener Minipig | broad_host_group | Sus scrofa | 9823 | review_before_apply | domestic pig/minipig host |
| 3 | Calathea sp. | taxonomy_exact_lookup | calathea | 4622 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | people | broad_host_group | Homo sapiens | 9606 | review_before_apply | human host |
| 3 | Populus | taxonomy_exact_lookup | populus | 3689 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | ferret | taxonomy_exact_lookup | ferret | 9669 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | pufferfish | broad_host_group | Tetraodontiformes | 31031 | review_before_apply | broad pufferfish host; avoids viral TaxonKit fuzzy false positive |
| 3 | Albino Bristlenose Pleco | broad_host_group | Ancistrus | 52070 | review_before_apply | bristlenose pleco host |
| 3 | Crasosstrea spp., Farfantepenaeus spp. | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 3 | sablefish, Anoplopoma fimbria | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 3 | Chrysanthemum | taxonomy_exact_lookup | chrysanthemum | 13422 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | Aster | taxonomy_exact_lookup | aster | 41479 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | lilac | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 3 | Asterionellopsis glacialis strain A3 | taxonomy_exact_lookup | asterionellopsis glacialis | 33640 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | Zhang Yuzhong | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 3 | Skeletonema marinoi strain ST54 | taxonomy_exact_lookup | skeletonema marinoi | 267567 | review_before_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | Discodermia calyx | manual_review_remaining |  |  | manual_validation_required | No conservative taxonomy/source/noise rule matched. |
| 3 | bamboo | taxonomy_exact_lookup | bamboo | 147376 | safe_to_apply | TaxonKit exact name lookup found a taxonomic identifier. |
| 3 | fowl | broad_host_group | Gallus gallus | 9031 | review_before_apply | domestic fowl/chicken host |

## Interpretation

The `safe_to_apply` decisions are conservative administrative, missing/noise, exact taxonomy, or non-host culture/source decisions.
The `review_before_apply` decisions are biologically plausible but broader than a species-level host, so they should be validated before becoming permanent rules.
The `manual_validation_required` decisions should remain in review unless another model or curator validates them.
