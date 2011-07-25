{-# LANGUAGE BangPatterns #-}
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

  map, fold, mapFold

) where

import Prelude                                  hiding ( map )

import GHC.Conc                                 ( numCapabilities )
import Control.Parallel
import Data.Vector.Unboxed                      ( Vector, Unbox )
import Data.Vector.Internal.Gang
import qualified Data.Vector.Unboxed            as U
import qualified Data.Vector.Unboxed.Mutable    as M


-- | Map a function to each element of an array.
--
{-# INLINE map #-}
map :: (Unbox a, Unbox b) => (a -> b) -> Vector a -> Vector b
map f vec
  | numCapabilities == 1        = U.map f vec
  | otherwise                   = U.create $
      do mvec <- M.unsafeNew len
         gangST theGang (\tid -> uncurry (fill mvec) (split tid))
         return mvec
    where
      len            = U.length vec
      pieces         = numCapabilities
      (step, remain) = len `quotRem` pieces

      {-# INLINE split #-}
      split i
        | i < remain = let offset = i * (step + 1)    in (offset, offset + step)
        | otherwise  = let offset = i * step + remain in (offset, offset + step - 1)

      {-# INLINE fill #-}
      fill !mvec !start !end = go start
        where go !i | i > end   = return ()
                    | otherwise = M.unsafeWrite mvec i (f $ U.unsafeIndex vec i) >> go (i+1)


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
fold :: Unbox a => (a -> a -> a) -> a -> Vector a -> a
fold c z vec
  | numCapabilities == 1        = U.foldl' c z vec
  | otherwise                   = reduce segs vec
    where
      len  = U.length vec
      segs = splitRange numCapabilities len

      {-# INLINE reduce #-}
      reduce []     _ = z
      reduce (s:ss) v =
        let (xs, ys) = U.splitAt s v
            x        = U.foldl' c z xs
            y        = reduce ss ys
        in
        x `par` y `pseq` c x y


-- | A combination of 'map' followed by 'fold', but computed more efficiently.
-- The same restrictions apply to the reduction operator and neutral element.
--
{-# INLINE mapFold #-}
mapFold :: (Unbox a, Unbox b) => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
mapFold f c z vec
  | numCapabilities == 1        = U.foldl' c z (U.map f vec)
  | otherwise                   = reduce segs vec
    where
      len  = U.length vec
      segs = splitRange numCapabilities len

      {-# INLINE reduce #-}
      reduce []     _ = z
      reduce (s:ss) v =
        let (xs, ys) = U.splitAt s v
            x        = U.foldl' c z (U.map f xs) -- will fuse
            y        = reduce ss ys
        in
        x `par` y `pseq` c x y


-- Auxiliary
-- ---------

-- Split a range into the given number of chunks as evenly as possible. Returns
-- a list of the size of each successive chunk.
--
splitRange :: Int -> Int -> [Int]
splitRange pieces len =
  replicate remain (step + 1) ++ replicate (pieces - remain) step
  where
    (step, remain) = len `quotRem` pieces

