-- |
-- Module      : Data.Vector.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Parallel (

  -- * Construction
  enumFromN, enumFromStepN,

  -- * Element-wise operations
  map, imap, zip, zipWith,

  -- * Reductions
  fold, foldMap,

  -- ** Specialised reductions
  all, any, and, or, sum, product, maximum, minimum,


  -- * Re-exported for convenience
  module Data.Vector

) where

import Prelude                                  ( Int, Bool, Num, Ord )
import Data.Vector                              hiding (
  enumFromN, enumFromStepN,
  map, imap, zip, zipWith,
  all, any, and, or, sum, product, maximum, minimum )
import qualified Data.Vector.Generic.Parallel   as G


-- Construction
-- ------------

-- | Yield a vector of the given length containing the values @x@, @x+1@ etc.
--
{-# INLINE enumFromN #-}
enumFromN :: Num a => a -> Int -> Vector a
enumFromN = G.enumFromN

-- | Yield a vector of the given values containing the values @x@, @x+y@,
-- @x+y+y@, etc.
--
{-# INLINE enumFromStepN #-}
enumFromStepN :: Num a => a -> a -> Int -> Vector a
enumFromStepN = G.enumFromStepN


-- Mapping
-- -------

-- | Map a function to each element of an array, in parallel.
--
{-# INLINE map #-}
map :: (a -> b) -> Vector a -> Vector b
map = G.map

-- | Map a function to each element of an array and its index.
--
{-# INLINE imap #-}
imap :: (Int -> a -> b) -> Vector a -> Vector b
imap = G.imap


-- Zipping
-- -------

-- | Zip two vectors
--
{-# INLINE zip #-}
zip :: Vector a -> Vector b -> Vector (a,b)
zip = G.zip

-- | Zip two vectors with the given function.
--
{-# INLINE zipWith #-}
zipWith :: (a -> b -> c) -> Vector a -> Vector b -> Vector c
zipWith = G.zipWith


-- Reductions
-- ----------

-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid. For example, @0@ is
-- neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: (a -> a -> a) -> a -> Vector a -> a
fold = G.fold

-- | A combination of 'map' followed by 'fold'. The same restrictions apply to
-- the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap = G.foldMap


-- Specialised reductions
-- ----------------------

-- | Check if all elements satisfy the predicate
--
{-# INLINE all #-}
all :: (a -> Bool) -> Vector a -> Bool
all = G.all

-- | Check if any element satisfies the predicate
--
{-# INLINE any #-}
any :: (a -> Bool) -> Vector a -> Bool
any = G.any

-- | Check if all elements are True
--
{-# INLINE and #-}
and :: Vector Bool -> Bool
and = G.and

-- | Check if any element is True
--
{-# INLINE or #-}
or :: Vector Bool -> Bool
or = G.or

-- | Compute the sum of the elements
--
{-# INLINE sum #-}
sum :: Num a => Vector a -> a
sum = G.sum

-- | Compute the product of the elements
--
{-# INLINE product #-}
product :: Num a => Vector a -> a
product = G.product

-- | Yield the maximum element of a non-empty vector
--
{-# INLINE maximum #-}
maximum :: Ord a => Vector a -> a
maximum = G.maximum

-- | Yield the minimum element of a non-empty vector
--
{-# INLINE minimum #-}
minimum :: Ord a => Vector a -> a
minimum = G.minimum

