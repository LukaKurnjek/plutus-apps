{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds   #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeOperators    #-}
module Plutus.Contract.ChainIndex.Server(
    serveChainIndex) where

import Control.Monad ((>=>))
import Control.Monad.Freer (Eff, Member, type (~>))
import Control.Monad.Freer.Error (Error, throwError)
import Data.Default (Default (def))
import Data.Maybe (fromMaybe)
import Plutus.Contract.ChainIndex.Api (API, FromHashAPI, QueryAtAddressRequest (QueryAtAddressRequest),
                                       TxoAtAddressRequest (TxoAtAddressRequest),
                                       UtxoAtAddressRequest (UtxoAtAddressRequest),
                                       UtxoWithCurrencyRequest (UtxoWithCurrencyRequest))
import Plutus.Contract.ChainIndex.Effects (ChainIndexControlEffect, ChainIndexQueryEffect)
import Plutus.Contract.ChainIndex.Effects qualified as E
import Servant.API ((:<|>) (..))
import Servant.API.ContentTypes (NoContent (..))
import Servant.Server (ServerError, ServerT, err404)

serveChainIndex ::
    forall effs.
    ( Member (Error ServerError) effs
    , Member ChainIndexQueryEffect effs
    , Member ChainIndexControlEffect effs
    )
    => ServerT API (Eff effs)
serveChainIndex =
    pure NoContent
    :<|> serveFromHashApi
    :<|> (E.txOutFromRef >=> handleMaybe)
    :<|> (E.unspentTxOutFromRef >=> handleMaybe)
    :<|> (E.txFromTxId >=> handleMaybe)
    :<|> E.utxoSetMembership
    :<|> (\(UtxoAtAddressRequest pq c) -> E.utxoSetAtAddress (fromMaybe def pq) c)
    :<|> (\(QueryAtAddressRequest pq c) -> E.unspentTxOutSetAtAddress (fromMaybe def pq) c)
    :<|> (\(QueryAtAddressRequest pq c) -> E.datumsAtAddress (fromMaybe def pq) c)
    :<|> (\(UtxoWithCurrencyRequest pq c) -> E.utxoSetWithCurrency (fromMaybe def pq) c)
    :<|> E.txsFromTxIds
    :<|> (\(TxoAtAddressRequest pq c) -> E.txoSetAtAddress (fromMaybe def pq) c)
    :<|> E.getTip
    :<|> E.collectGarbage *> pure NoContent
    :<|> E.getDiagnostics

serveFromHashApi ::
    forall effs.
    ( Member (Error ServerError) effs
    , Member ChainIndexQueryEffect effs
    )
    => ServerT FromHashAPI (Eff effs)
serveFromHashApi =
    (E.datumFromHash >=> handleMaybe)
    :<|> (E.validatorFromHash >=> handleMaybe)
    :<|> (E.mintingPolicyFromHash >=> handleMaybe)
    :<|> (E.stakeValidatorFromHash >=> handleMaybe)
    :<|> (E.redeemerFromHash >=> handleMaybe)

-- | Return the value of throw a 404 error
handleMaybe :: forall effs. Member (Error ServerError) effs => Maybe ~> Eff effs
handleMaybe = maybe (throwError err404) pure
