-- |
-- Module      : Data.Vector.Unboxed.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Unboxed.Parallel (

  map, fold, foldMap

) where

import Prelude                                  hiding ( map )
import Control.DeepSeq                          ( NFData )
import Data.Vector.Unboxed                      ( Vector, Unbox )
import qualified Data.Vector.Generic.Parallel   as G


-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: (Unbox a, Unbox b, NFData b) => (a -> b) -> Vector a -> Vector b
map = G.map


-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid.
--
-- For example, @0@ is neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: (Unbox a, NFData a) => (a -> a -> a) -> a -> Vector a -> a
fold = G.fold


-- | A combination of 'map' followed by 'fold', but computed more efficiently.
-- The same restrictions apply to the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (Unbox a, NFData b) => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap = G.foldMap

{-# RULES
"map/map"       forall f g.     map f . map g         = map (f . g)
"fold/map"      forall f c z.   fold c z . map f      = foldMap f c z
"mapFold/map"   forall f g c z. foldMap f c z . map g = foldMap (f . g) c z
  #-}

